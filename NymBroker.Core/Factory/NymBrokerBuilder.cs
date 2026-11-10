using NymBroker.Core.Consume;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.File;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Factory.Configuration;
using NymBroker.Core.Filter;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.PubSub;
using NymBroker.Core.Route;
using NymBroker.Core.Transform;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Factory;

public sealed partial class NymBrokerBuilder
{
    private readonly IServiceCollection _services;
    private readonly List<string> _endpoints = [];
    private readonly List<(Type ConsumerType, Type MessageType, bool RetryOnFailure)> _consumers = [];
    private readonly List<TopicContext> _topicContexts = [];
    private readonly List<TopicConfiguration> _configTopics = [];
    private readonly List<(Type TransformerType, string? Endpoint)> _transformers = [];
    private readonly List<(Type TransformerType, string Endpoint)> _outputTransformers = [];
    private readonly List<RouteContext> _routes = [];
    private readonly List<IMessageFilter> _filterInstances = [];
    private readonly List<Action<NymBrokerImpl, IServiceProvider>> _scheduledActions = [];
    private readonly HashSet<string> _scheduledActionNames = new(StringComparer.OrdinalIgnoreCase);
    private bool _scheduleStore;
    private readonly List<Type> _builderFilterTypes = [];
    private readonly List<string> _wireTapEndpoints = [];
    private string? _deadLetterEndpoint;
    private TimeSpan? _maxMessageAge;
    private readonly BrokerHealthCheckOptions _healthCheckOptions = new();
    private bool _idempotentReceiver;
    private bool _built;

    /// <summary>Exposes the DI container for endpoint extension packages (e.g. NymBroker.Endpoint.RabbitMq).</summary>
    public IServiceCollection Services => _services;

    /// <summary>Set by <see cref="LoadConfiguration"/> so extension packages can process their endpoint types.</summary>
    public BrokerConfiguration? LoadedConfiguration { get; private set; }

    public NymBrokerBuilder(IServiceCollection services) => _services = services;

    // --- Endpoint registration ---

    public NymBrokerBuilder AddFileEndPoint(string name, FileSettings? settings = null, EndpointMode mode = EndpointMode.ReadWrite)
    {
        var s = settings ?? new FileSettings();
        _services.AddKeyedSingleton<IEndPoint>(name, (sp, _) => new FileEndPoint(name, s, sp.GetRequiredService<ILogger<FileEndPoint>>(), mode));
        _endpoints.Add(name);
        return this;
    }

    public NymBrokerBuilder AddMemoryEndPoint(string name, int capacity = 1000, EndpointMode mode = EndpointMode.ReadWrite)
    {
        _services.AddKeyedSingleton<IEndPoint>(name, (sp, _) => new MemoryQueueEndPoint(name, capacity, sp.GetRequiredService<ILogger<MemoryQueueEndPoint>>(), mode));
        _endpoints.Add(name);
        return this;
    }

    /// <summary>Registers an endpoint name that was added externally (e.g. by an extension package).</summary>
    public void RegisterEndpoint(string name) => _endpoints.Add(name);

    // --- Consumer registration ---

    public NymBrokerBuilder AddConsumer<TConsumer>() where TConsumer : class, IMessageConsumer
        => AddConsumer<TConsumer>(new ConsumerOptions());

    /// <summary>
    /// Registers a consumer with options, e.g. <c>o =&gt; o.RetryOnFailure = false</c> to dead-letter a message at once
    /// when the consumer throws. The options apply to every message type the consumer handles.
    /// </summary>
    public NymBrokerBuilder AddConsumer<TConsumer>(Action<ConsumerOptions> configure) where TConsumer : class, IMessageConsumer
    {
        ArgumentNullException.ThrowIfNull(configure);
        var options = new ConsumerOptions();
        configure(options);
        return AddConsumer<TConsumer>(options);
    }

    private NymBrokerBuilder AddConsumer<TConsumer>(ConsumerOptions options) where TConsumer : class, IMessageConsumer
    {
        var messageTypes = typeof(TConsumer).GetInterfaces()
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IConsume<>))
            .Select(i => i.GetGenericArguments()[0])
            .Distinct()
            .ToList();

        if (messageTypes.Count == 0)
            throw new InvalidOperationException($"{typeof(TConsumer).Name} must implement IConsume<T>.");

        // Registering the same consumer twice is harmless, unless the options disagree.
        var registered = _consumers.FirstOrDefault(c => c.ConsumerType == typeof(TConsumer));
        if (registered.ConsumerType is not null)
        {
            if (registered.RetryOnFailure != options.RetryOnFailure)
                throw new InvalidOperationException(
                    $"Consumer '{typeof(TConsumer).Name}' is already registered with RetryOnFailure = {registered.RetryOnFailure}; " +
                    "register it once, with the options it should have.");
            return this;
        }

        // Consumers are keyed services by class name, so two different classes with the same name would collide.
        var sameName = _consumers.FirstOrDefault(c => c.ConsumerType.Name == typeof(TConsumer).Name);
        if (sameName.ConsumerType is not null)
            throw new InvalidOperationException(
                $"Consumers '{sameName.ConsumerType.FullName}' and '{typeof(TConsumer).FullName}' have the same class name; " +
                "consumers are registered by class name, so give one of them a different name.");

        // A message type has exactly one consumer: a second one would silently replace the first.
        foreach (var messageType in messageTypes)
        {
            var existing = _consumers.FirstOrDefault(c => c.MessageType == messageType);
            if (existing.ConsumerType is not null)
                throw DuplicateConsumer(messageType, existing.ConsumerType.Name, typeof(TConsumer).Name);
        }

        foreach (var messageType in messageTypes)
            _consumers.Add((typeof(TConsumer), messageType, options.RetryOnFailure));

        _services.AddKeyedTransient<IMessageConsumer, TConsumer>(typeof(TConsumer).Name);
        return this;
    }

    internal static InvalidOperationException DuplicateConsumer(Type messageType, string existingConsumer, string newConsumer)
        => new($"Message type '{MessageTypeName.Get(messageType)}' already has consumer '{existingConsumer}', so '{newConsumer}' " +
               "cannot also consume it — each message type has exactly one consumer. " +
               "To handle a message in several places, use a topic with ISubscribe<T> subscribers (AddTopic<T>(...).SubscribeWith<...>()).");

    // --- Pub/Sub topic registration ---

    /// <summary>Begins a fluent topic definition for message type T.</summary>
    public ITopicBuilder<T> AddTopic<T>(string topicName) where T : class
        => new TopicBuilder<T>(topicName, ctx => _topicContexts.Add(ctx), this);

    /// <summary>Registers an <see cref="ISubscribe{T}"/> implementation as a keyed DI service.</summary>
    public NymBrokerBuilder AddSubscriber<TSubscriber>() where TSubscriber : class, IMessageSubscriber
    {
        _services.AddKeyedTransient<IMessageSubscriber, TSubscriber>(typeof(TSubscriber).Name);
        return this;
    }

    // --- Input transformer registration ---

    public NymBrokerBuilder AddInputTransformer<TTransformer>(string? endpoint = null)
        where TTransformer : class, IInputTransformer
    {
        _services.TryAddTransient<TTransformer>();
        _transformers.Add((typeof(TTransformer), endpoint));
        return this;
    }

    /// <summary>Transform every message sent to <paramref name="endpoint"/> into the destination's own format, e.g. XML.</summary>
    public NymBrokerBuilder AddOutputTransformer<TTransformer>(string endpoint)
        where TTransformer : class, IOutputTransformer
    {
        ArgumentException.ThrowIfNullOrEmpty(endpoint);
        _services.TryAddTransient<TTransformer>();
        _outputTransformers.Add((typeof(TTransformer), endpoint));
        return this;
    }

    // --- Routes, filters, scheduled actions ---

    /// <summary>
    /// Route messages of type <typeparamref name="T"/>: <c>.AddRoute&lt;Order&gt;(r =&gt; r.To("Orders").WhenFrom("In"))</c>.
    /// The route is checked here (it needs <c>To</c>); <c>StartAsync</c> checks that the endpoints exist.
    /// </summary>
    public NymBrokerBuilder AddRoute<T>(Action<IRouteBuilder<T>> configure) where T : class
        => AddRoute(new RouteBuilder<T>(static _ => { }), configure);

    /// <summary>Route messages of any type: <c>.AddRoute(r =&gt; r.To("Audit"))</c>.</summary>
    public NymBrokerBuilder AddRoute(Action<IRouteBuilder<IAnyMessage>> configure)
        => AddRoute(new RouteBuilder<IAnyMessage>(static _ => { }), configure);

    /// <summary>Route with a custom <see cref="RouteContext"/> (a subclass overriding <c>Evaluate</c>), refined by <paramref name="configure"/>.</summary>
    public NymBrokerBuilder AddRoute(Func<RouteContext> routeContextFactory, Action<IRouteBuilder<IAnyMessage>> configure)
    {
        ArgumentNullException.ThrowIfNull(routeContextFactory);
        return AddRoute(new RouteBuilder<IAnyMessage>(static _ => { }, routeContextFactory), configure);
    }

    private NymBrokerBuilder AddRoute<T>(RouteBuilder<T> route, Action<IRouteBuilder<T>> configure) where T : class
    {
        ArgumentNullException.ThrowIfNull(configure);
        configure(route);
        _routes.Add(route.Build());   // throws now if To(...) is missing
        return this;
    }

    /// <summary>Run <paramref name="filter"/> for every received message, in the order filters are added.</summary>
    public NymBrokerBuilder AddFilter(IMessageFilter filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        _filterInstances.Add(filter);
        return this;
    }

    /// <summary>Run a filter resolved from DI (a singleton) for every received message.</summary>
    public NymBrokerBuilder AddFilter<TFilter>() where TFilter : class, IMessageFilter
    {
        _services.TryAddSingleton<TFilter>();
        _builderFilterTypes.Add(typeof(TFilter));
        return this;
    }

    /// <summary>
    /// Configures <see cref="INymBroker.CheckHealthAsync"/>: the overall timeout and which endpoints are non-critical
    /// (an unhealthy non-critical endpoint makes the broker Degraded instead of Unhealthy). Can be called more than once.
    /// </summary>
    public NymBrokerBuilder ConfigureHealthCheck(Action<BrokerHealthCheckOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        configure(_healthCheckOptions);
        return this;
    }

    // --- Load from config file ---

    public NymBrokerBuilder LoadConfiguration(string filePath)
        => ApplyConfiguration(BrokerConfigurationReader.Read(filePath));

    /// <summary>
    /// Registers the File and Memory endpoints and the topics of <paramref name="config"/>, and keeps it as
    /// <see cref="LoadedConfiguration"/> so the transport packages' <c>With…()</c> calls that follow register theirs.
    /// </summary>
    public NymBrokerBuilder ApplyConfiguration(BrokerConfiguration config)
    {
        ArgumentNullException.ThrowIfNull(config);
        LoadedConfiguration = config;
        foreach (var ep in config.Endpoints)
        {
            if (ep.IsType(EndPointType.File)) AddFileEndPoint(ep.Name, ep.ToFileSettings(), ep.Mode);
            else if (ep.IsType(EndPointType.Memory)) AddMemoryEndPoint(ep.Name, mode: ep.Mode);
            // Every other type (RabbitMq, Sql, Postgres, custom) is handled by its package's With*() extension.
        }

        foreach (var topic in config.Topics)
            _configTopics.Add(topic);

        return this;
    }

    /// <summary>
    /// Calls <paramref name="register"/> for every endpoint of type <paramref name="type"/> (case-insensitive) in
    /// <see cref="LoadedConfiguration"/>; does nothing when no configuration was loaded. For transport packages:
    /// <c>builder.AddConfiguredEndPoints("Kafka", ep =&gt; builder.AddKafkaEndPoint(ep.Name, ep.GetSettings&lt;KafkaSettings&gt;(), ep.Mode))</c>.
    /// </summary>
    public NymBrokerBuilder AddConfiguredEndPoints(string type, Action<EndPointConfiguration> register)
    {
        ArgumentNullException.ThrowIfNull(register);
        if (LoadedConfiguration is null) return this;

        foreach (var ep in LoadedConfiguration.Endpoints)
        {
            if (ep.IsType(type))
                register(ep);
        }
        return this;
    }

    // --- Build ---
}
