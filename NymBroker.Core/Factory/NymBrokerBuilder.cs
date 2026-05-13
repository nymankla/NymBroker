using System.Collections.Immutable;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Consume;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.File;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Factory.Configuration;
using NymBroker.Core.DI;
using NymBroker.Core.Filter;
using NymBroker.Core.Idempotency;
using NymBroker.Core.Impl;
using NymBroker.Core.PubSub;
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
    private readonly List<(Type ConsumerType, Type MessageType)> _consumers = [];
    private readonly List<TopicContext> _topicContexts = [];
    private readonly List<TopicConfiguration> _configTopics = [];
    private readonly List<(Type TransformerType, string? Endpoint)> _transformers = [];
    private readonly List<Type> _builderFilterTypes = [];
    private readonly List<string> _wireTapEndpoints = [];
    private string? _deadLetterEndpoint;
    private TimeSpan? _maxMessageAge;
    private bool _built;

    /// <summary>Exposes the DI container for endpoint extension packages (e.g. NymBroker.RabbitMq).</summary>
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
    {
        var messageTypes = typeof(TConsumer).GetInterfaces()
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IConsume<>))
            .Select(i => i.GetGenericArguments()[0])
            .Distinct()
            .ToList();

        if (messageTypes.Count == 0)
            throw new InvalidOperationException($"{typeof(TConsumer).Name} must implement IConsume<T>.");

        foreach (var messageType in messageTypes)
            _consumers.Add((typeof(TConsumer), messageType));

        _services.AddKeyedTransient(typeof(IMessageConsumer), typeof(TConsumer).Name, typeof(TConsumer));
        return this;
    }

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

    /// <summary>Register an idempotent receiver that drops duplicate message IDs within the TTL window.</summary>
    public NymBrokerBuilder AddIdempotentReceiver(TimeSpan? ttl = null)
    {
        _services.AddSingleton<IIdempotencyStore>(new InMemoryIdempotencyStore(ttl ?? TimeSpan.FromHours(24)));
        _services.AddSingleton<IdempotentFilter>();
        _builderFilterTypes.Add(typeof(IdempotentFilter));
        return this;
    }

    // --- Load from config file ---

    public NymBrokerBuilder LoadConfiguration(string filePath)
    {
        LoadedConfiguration = BrokerConfigurationReader.Read(filePath);
        return ApplyConfiguration(LoadedConfiguration);
    }

    public NymBrokerBuilder ApplyConfiguration(BrokerConfiguration config)
    {
        foreach (var ep in config.Endpoints)
        {
            switch (ep.Type)
            {
                case EndPointType.File: AddFileEndPoint(ep.Name, ep.ToFileSettings(), ep.Mode); break;
                case EndPointType.Memory: AddMemoryEndPoint(ep.Name, mode: ep.Mode); break;
                // EndPointType.RabbitMq is handled by NymBroker.RabbitMq via WithRabbitMq()
            }
        }

        foreach (var topic in config.Topics)
            _configTopics.Add(topic);

        return this;
    }

    // --- Build ---

    public void Build()
    {
        if (_built)
            throw new InvalidOperationException("NymBrokerBuilder.Build() can only be called once.");

        _built = true;

        _services.AddSingleton<MessageSerializerJson>();
        _services.AddSingleton<IMessageSerializer>(sp => sp.GetRequiredService<MessageSerializerJson>());
        _services.AddSingleton<IAggregator, AggregatorImpl>();
        _services.AddSingleton<ISplitter, SplitterImpl>();
        _services.AddSingleton<MessageTypeRegistry>();
        _services.AddSingleton<ConsumerDispatcher>();
        _services.AddSingleton<SubscriberDispatcher>();

        // Capture lists for closure.
        var endpoints        = _endpoints.ToList();
        var consumers        = _consumers.ToList();
        var topicContexts    = _topicContexts.ToList();
        var configTopics     = _configTopics.ToList();
        var transformers     = _transformers.ToList();
        var builderFilters   = _builderFilterTypes.ToList();
        var wireTapEndpoints = _wireTapEndpoints.ToList();
        var deadLetter       = _deadLetterEndpoint;
        var maxMessageAge    = _maxMessageAge;

        _services.AddSingleton<NymBrokerImpl>(sp =>
        {
            var broker = new NymBrokerImpl(
                sp.GetRequiredService<MessageSerializerJson>(),
                sp.GetRequiredService<IAggregator>(),
                sp.GetRequiredService<MessageTypeRegistry>(),
                sp.GetRequiredService<ConsumerDispatcher>(),
                sp.GetRequiredService<SubscriberDispatcher>(),
                sp.GetRequiredService<ILogger<NymBrokerImpl>>());

            foreach (var endpointName in endpoints)
                broker.AddEndpoint(endpointName, sp.GetRequiredKeyedService<IEndPoint>(endpointName));

            foreach (var (consumerType, messageType) in consumers)
                broker.RegisterConsumer(messageType, consumerType.Name);

            foreach (var topic in topicContexts)
                broker.AddTopic(topic);

            foreach (var (type, endpoint) in transformers)
                broker.AddInputTransformer((IInputTransformer)sp.GetRequiredService(type), endpoint);

            foreach (var filterType in builderFilters)
                broker.AddFilter((IMessageFilter)sp.GetRequiredService(filterType));

            foreach (var tap in wireTapEndpoints)
                broker.AddWireTap(tap);

            if (deadLetter != null)
                broker.SetDeadLetterEndpoint(deadLetter);

            if (maxMessageAge.HasValue)
                broker.SetMaxMessageAge(maxMessageAge.Value);

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
