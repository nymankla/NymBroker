using System.Collections.Immutable;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Consume;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.File;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Factory.Configuration;
using NymBroker.Core.DI;
using NymBroker.Core.Filter;
using NymBroker.Core.Idempotency;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.PubSub;
using NymBroker.Core.Route;
using NymBroker.Core.Scheduling;
using NymBroker.Core.Serialize;
using NymBroker.Core.Splitter;
using NymBroker.Core.Transform;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Factory;

public sealed class NymBrokerBuilder
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

        _services.AddKeyedTransient(typeof(IMessageConsumer), typeof(TConsumer).Name, typeof(TConsumer));
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
        _services.AddKeyedTransient(typeof(IMessageSubscriber), typeof(TSubscriber).Name, typeof(TSubscriber));
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
    /// Run <paramref name="action"/> every <paramref name="interval"/> while the broker runs, first after one interval.
    /// Each run gets its own DI scope; a failing run is logged and the schedule continues.
    /// </summary>
    public NymBrokerBuilder AddScheduledAction(TimeSpan interval, Func<IServiceProvider, CancellationToken, Task> action)
    {
        if (interval <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(interval), "The interval must be positive.");
        ArgumentNullException.ThrowIfNull(action);
        _scheduledActions.Add((broker, sp) => broker.AddScheduledAction(interval, ct => RunScopedAsync(sp, action, ct)));
        return this;
    }

    /// <summary>
    /// Run <paramref name="action"/> on a cron schedule (five fields or a macro such as <c>@daily</c>, local time zone) while the broker runs.
    /// Each run gets its own DI scope; a failing run is logged and the schedule continues.
    /// </summary>
    public NymBrokerBuilder AddScheduledAction(string cronExpression, Func<IServiceProvider, CancellationToken, Task> action)
    {
        ArgumentException.ThrowIfNullOrEmpty(cronExpression);
        ArgumentNullException.ThrowIfNull(action);
        _ = CronSchedule.Parse(cronExpression);   // an invalid expression throws here, not at startup
        _scheduledActions.Add((broker, sp) => broker.AddScheduledAction(cronExpression, ct => RunScopedAsync(sp, action, ct)));
        return this;
    }

    // RS0026: the two named overloads differ in a required parameter (TimeSpan interval / string cron), so calls are never ambiguous.
#pragma warning disable RS0026
    /// <summary>
    /// Run a named action every <paramref name="interval"/> while the broker runs. Occurrences are on a fixed grid
    /// (multiples of the interval since 1970-01-01 UTC) and each is claimed in the schedule store before it runs:
    /// brokers sharing a store (<see cref="AddScheduleStore"/>) run it once per occurrence, and
    /// <see cref="ScheduledActionOptions.MissedRuns"/> can catch up a run missed while the broker was down.
    /// </summary>
    /// <param name="name">Unique (case-insensitive) key of the action in the schedule store; at most 200 characters.</param>
    /// <param name="interval">Positive, whole milliseconds.</param>
    public NymBrokerBuilder AddScheduledAction(string name, TimeSpan interval, Func<IServiceProvider, CancellationToken, Task> action, Action<ScheduledActionOptions>? configure = null)
        => AddNamedScheduledAction(name, new GridTiming(interval), action, configure);

    /// <summary>
    /// Run a named action on a cron schedule (local time zone) while the broker runs. Each occurrence is claimed in the
    /// schedule store before it runs: brokers sharing a store (<see cref="AddScheduleStore"/>) run it once per
    /// occurrence, and <see cref="ScheduledActionOptions.MissedRuns"/> can catch up a run missed while the broker was down.
    /// </summary>
    /// <param name="name">Unique (case-insensitive) key of the action in the schedule store; at most 200 characters.</param>
    public NymBrokerBuilder AddScheduledAction(string name, string cronExpression, Func<IServiceProvider, CancellationToken, Task> action, Action<ScheduledActionOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(cronExpression);
        return AddNamedScheduledAction(name, new CronTiming(CronSchedule.Parse(cronExpression)), action, configure);
    }
#pragma warning restore RS0026

    private NymBrokerBuilder AddNamedScheduledAction(string name, ScheduleTiming timing, Func<IServiceProvider, CancellationToken, Task> action, Action<ScheduledActionOptions>? configure)
    {
        ScheduledActionName.Validate(name);
        ArgumentNullException.ThrowIfNull(action);
        var options = new ScheduledActionOptions();
        configure?.Invoke(options);
        options.Validate();
        if (!_scheduledActionNames.Add(name))
            throw new ArgumentException($"A scheduled action named '{name}' is already added.", nameof(name));

        var (missedRuns, maxCatchUp) = (options.MissedRuns, options.MaxCatchUp);
        _scheduledActions.Add((broker, sp) => broker.AddNamedScheduledAction(name, timing, missedRuns, maxCatchUp, ct => RunScopedAsync(sp, action, ct)));
        return this;
    }

    /// <summary>
    /// Where named scheduled actions record their runs: a store shared by several instances makes each occurrence run
    /// once, and a durable one lets <see cref="MissedRunPolicy.RunOnce"/> catch up across restarts. The
    /// <c>NymBroker.Scheduling.*</c> packages call this. Without it, an in-process store is used. The last registration wins.
    /// </summary>
    public NymBrokerBuilder AddScheduleStore(Func<IServiceProvider, IScheduleStore> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        _services.RemoveAll<IScheduleStore>();
        _services.AddSingleton(factory);
        _scheduleStore = true;
        return this;
    }

    private static async Task RunScopedAsync(IServiceProvider services, Func<IServiceProvider, CancellationToken, Task> action, CancellationToken ct)
    {
        await using var scope = services.CreateAsyncScope();
        await action(scope.ServiceProvider, ct);
    }

    // --- Dead letter, wire tap, TTL, idempotency ---

    /// <summary>Forward failed and expired messages to this endpoint.</summary>
    public NymBrokerBuilder WithDeadLetterEndpoint(string endpointName)
    {
        _deadLetterEndpoint = endpointName;
        return this;
    }

    /// <summary>Copy every incoming raw message to this endpoint before processing.</summary>
    public NymBrokerBuilder AddWireTap(string endpointName)
    {
        _wireTapEndpoints.Add(endpointName);
        return this;
    }

    /// <summary>Discard (and dead-letter if configured) messages older than maxAge.</summary>
    public NymBrokerBuilder DiscardMessagesOlderThan(TimeSpan maxAge)
    {
        _maxMessageAge = maxAge;
        return this;
    }

    /// <summary>Log every incoming message at Debug level before routing and consumer dispatch.</summary>
    public NymBrokerBuilder AddMessageLoggingFilter()
    {
        _services.AddSingleton<LoggingFilter>();
        _builderFilterTypes.Add(typeof(LoggingFilter));
        return this;
    }

    /// <summary>
    /// Register an idempotent receiver with the in-memory store: duplicate message IDs within the TTL window are
    /// dropped (per process). For a durable store shared by several instances use a database store package
    /// (e.g. <c>AddSqlServerIdempotency</c>) or <see cref="AddIdempotentReceiver(IIdempotencyStore)"/>.
    /// </summary>
    public NymBrokerBuilder AddIdempotentReceiver(TimeSpan? ttl = null)
        => AddIdempotentReceiver(new InMemoryIdempotencyStore(ttl ?? TimeSpan.FromHours(24)));

    /// <summary>Register an idempotent receiver backed by <paramref name="store"/>.</summary>
    public NymBrokerBuilder AddIdempotentReceiver(IIdempotencyStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        return AddIdempotentReceiver(_ => store);
    }

    /// <summary>Register an idempotent receiver backed by a store resolved from DI (e.g. registered by an extension package).</summary>
    public NymBrokerBuilder AddIdempotentReceiver<TStore>() where TStore : class, IIdempotencyStore
    {
        _services.TryAddSingleton<TStore>();
        return AddIdempotentReceiver(sp => sp.GetRequiredService<TStore>());
    }

    /// <summary>Register an idempotent receiver backed by the store <paramref name="factory"/> creates. One store per broker; the last registration wins.</summary>
    public NymBrokerBuilder AddIdempotentReceiver(Func<IServiceProvider, IIdempotencyStore> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        _services.RemoveAll<IIdempotencyStore>();
        _services.AddSingleton(factory);
        _idempotentReceiver = true;
        return this;
    }

    /// <summary>
    /// Register an idempotent receiver backed by a database store that needs periodic cleanup (what the
    /// <c>NymBroker.Idempotency.*</c> packages call). <paramref name="factory"/> creates the store once (singleton); when
    /// <paramref name="cleanupInterval"/> is positive a hosted service calls
    /// <see cref="IExpiringIdempotencyStore.DeleteExpiredAsync"/> on that interval, logging failures and retrying at the
    /// next one. One store per broker; the last registration wins.
    /// </summary>
    /// <param name="tableName">Where the entries live, for log messages.</param>
    public NymBrokerBuilder AddIdempotencyStore<TStore>(Func<IServiceProvider, TStore> factory, TimeSpan cleanupInterval, string tableName)
        where TStore : class, IExpiringIdempotencyStore
    {
        ArgumentNullException.ThrowIfNull(factory);

        _services.RemoveAll<TStore>();
        _services.AddSingleton(factory);
        AddIdempotentReceiver(sp => sp.GetRequiredService<TStore>());

        // Replace an earlier registration of the same store type, including its cleanup service.
        _services.RemoveAll<IdempotencyCleanupOptions<TStore>>();
        for (var i = _services.Count - 1; i >= 0; i--)
        {
            if (_services[i].ServiceType == typeof(IHostedService) && _services[i].ImplementationType == typeof(IdempotencyCleanupService<TStore>))
                _services.RemoveAt(i);
        }

        if (cleanupInterval > TimeSpan.Zero)
        {
            _services.AddSingleton(new IdempotencyCleanupOptions<TStore>(cleanupInterval, tableName));
            _services.AddSingleton<IHostedService, IdempotencyCleanupService<TStore>>();
        }
        return this;
    }

    // --- Health check ---

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

    public void Build()
    {
        if (_built)
            throw new InvalidOperationException("NymBrokerBuilder.Build() can only be called once.");

        var unknownNonCritical = _healthCheckOptions.NonCriticalEndpoints
            .Where(n => !_endpoints.Contains(n, StringComparer.OrdinalIgnoreCase))
            .ToList();
        if (unknownNonCritical.Count > 0)
            throw new InvalidOperationException(
                $"ConfigureHealthCheck marks unknown endpoint(s) as non-critical: {string.Join(", ", unknownNonCritical)}. " +
                "Register the endpoint, or remove it from NonCritical(...).");

        _built = true;

        _services.AddSingleton<MessageSerializerJson>();
        _services.AddSingleton<IMessageSerializer>(sp => sp.GetRequiredService<MessageSerializerJson>());
        _services.AddSingleton<IAggregator, AggregatorImpl>();
        _services.AddSingleton<ISplitter, SplitterImpl>();
        _services.AddSingleton<ICompressor, BrotliCompressor>();
        _services.AddSingleton<MessageTypeRegistry>();
        _services.AddSingleton<ConsumerDispatcher>();
        _services.AddSingleton<SubscriberDispatcher>();

        // Capture lists for closure.
        var endpoints        = _endpoints.ToList();
        var consumers        = _consumers.ToList();
        var topicContexts    = _topicContexts.ToList();
        var configTopics     = _configTopics.ToList();
        var transformers     = _transformers.ToList();
        var outputTransformers = _outputTransformers.ToList();
        var builderFilters   = _builderFilterTypes.ToList();
        var filterInstances  = _filterInstances.ToList();
        var routes           = _routes.ToList();
        var scheduledActions = _scheduledActions.ToList();
        var wireTapEndpoints = _wireTapEndpoints.ToList();
        var deadLetter       = _deadLetterEndpoint;
        var maxMessageAge    = _maxMessageAge;
        var idempotent       = _idempotentReceiver;
        var scheduleStore    = _scheduleStore;
        var healthCheck      = _healthCheckOptions;

        _services.AddSingleton<NymBrokerImpl>(sp =>
        {
            var broker = new NymBrokerImpl(
                sp.GetRequiredService<MessageSerializerJson>(),
                sp.GetRequiredService<IAggregator>(),
                sp.GetRequiredService<MessageTypeRegistry>(),
                sp.GetRequiredService<ConsumerDispatcher>(),
                sp.GetRequiredService<SubscriberDispatcher>(),
                sp.GetRequiredService<ILogger<NymBrokerImpl>>(),
                sp.GetRequiredService<ISplitter>(),
                sp.GetRequiredService<ICompressor>());

            foreach (var endpointName in endpoints)
                broker.AddEndpoint(endpointName, sp.GetRequiredKeyedService<IEndPoint>(endpointName));

            foreach (var (consumerType, messageType, retryOnFailure) in consumers)
                broker.RegisterConsumer(messageType, consumerType.Name, retryOnFailure);

            foreach (var topic in topicContexts)
                broker.AddTopic(topic);

            foreach (var (type, endpoint) in transformers)
                broker.AddInputTransformer((IInputTransformer)sp.GetRequiredService(type), endpoint);

            foreach (var (type, endpoint) in outputTransformers)
                broker.AddOutputTransformer((IOutputTransformer)sp.GetRequiredService(type), endpoint);

            foreach (var filterType in builderFilters)
                broker.AddFilter((IMessageFilter)sp.GetRequiredService(filterType));

            foreach (var filter in filterInstances)
                broker.AddFilter(filter);

            foreach (var route in routes)
                broker.AddRoute(route);

            if (scheduleStore)
                broker.SetScheduleStore(sp.GetRequiredService<IScheduleStore>());

            foreach (var schedule in scheduledActions)
                schedule(broker, sp);

            foreach (var tap in wireTapEndpoints)
                broker.AddWireTap(tap);

            if (deadLetter != null)
                broker.SetDeadLetterEndpoint(deadLetter);

            if (maxMessageAge.HasValue)
                broker.SetMaxMessageAge(maxMessageAge.Value);

            if (idempotent)
                broker.SetIdempotencyStore(sp.GetRequiredService<IIdempotencyStore>());

            broker.ConfigureHealthCheck(healthCheck);

            // Config-based topics: resolve message type string → CLR type via registry.
            var registry = sp.GetRequiredService<MessageTypeRegistry>();
            foreach (var ct in configTopics)
            {
                var msgType = ct.MessageType != null
                    ? registry.Resolve(ct.MessageType) ?? typeof(Message.IAnyMessage)
                    : typeof(Message.IAnyMessage);
                broker.AddTopic(new TopicContext
                {
                    TopicName = ct.TopicName,
                    MessageType = msgType,
                    SubscriberEndpoints = ct.SubscriberEndpoints.ToImmutableList()
                });
            }

            return broker;
        });

        _services.AddSingleton<INymBroker>(sp => sp.GetRequiredService<NymBrokerImpl>());
        _services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, NymBrokerHostedService>());
    }
}
