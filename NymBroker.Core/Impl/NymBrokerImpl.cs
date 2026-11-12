using System.Collections.Frozen;
using System.Collections.Immutable;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Consume;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Filter;
using NymBroker.Core.Message;
using NymBroker.Core.Transform;
using NymBroker.Core.PubSub;
using NymBroker.Core.Route;
using NymBroker.Core.Serialize;
using NymBroker.Core.Splitter;
using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Impl;

internal sealed partial class NymBrokerImpl : INymBroker
{
    private readonly MessageSerializerJson _serializer;
    private readonly IAggregator _aggregator;
    private readonly ISplitter _splitter;
    private readonly ICompressor _compressor;
    private readonly MessageTypeRegistry _messageTypeRegistry;
    private readonly ConsumerDispatcher _consumerDispatcher;
    private readonly SubscriberDispatcher _subscriberDispatcher;
    private readonly ILogger<NymBrokerImpl> _logger;
    private readonly SemaphoreSlim _lifecycleLock = new(1, 1);
    private readonly TaskCompletionSource _startGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
    // Scheduled actions: registered start functions, the handles currently running, and whether they run.
    // All three are guarded by _scheduleLock (never held across an await).
    private readonly Lock _scheduleLock = new();
    private ImmutableList<Func<CancellationToken, ScheduledActionHandle>> _scheduledActions = ImmutableList<Func<CancellationToken, ScheduledActionHandle>>.Empty;
    private ImmutableList<ScheduledActionHandle> _activeScheduledActions = ImmutableList<ScheduledActionHandle>.Empty;
    private bool _scheduledActionsRunning;
    private volatile bool _startInitiated;
    private bool _started;

    // Thread-safe via replacement: writes happen at configuration time, reads for every message, so these are the
    // collections that are fastest to read.
    private ImmutableArray<RouteContext> _routes = [];
    private ImmutableArray<IMessageFilter> _filters = [];
    private ImmutableArray<TopicContext> _topics = [];
    // Message types whose consumer was registered with RetryOnFailure = false.
    private ImmutableHashSet<Type> _noRetryMessageTypes = ImmutableHashSet<Type>.Empty;
    private FrozenDictionary<string, IInputTransformer> _endpointTransformers = EmptyRegistry<IInputTransformer>();
    private IInputTransformer? _globalInputTransformer;
    private FrozenDictionary<string, IOutputTransformer> _outputTransformers = EmptyRegistry<IOutputTransformer>();
    private ImmutableArray<string> _wireTapEndpoints = [];
    private string? _deadLetterEndpoint;
    private TimeSpan? _maxMessageAge;

    private FrozenDictionary<string, IEndPoint> _endpoints = EmptyRegistry<IEndPoint>();

    public NymBrokerImpl(
        MessageSerializerJson serializer,
        IAggregator aggregator,
        MessageTypeRegistry messageTypeRegistry,
        ConsumerDispatcher consumerDispatcher,
        SubscriberDispatcher subscriberDispatcher,
        ILogger<NymBrokerImpl> logger,
        ISplitter? splitter = null,
        ICompressor? compressor = null)
    {
        _serializer = serializer;
        _aggregator = aggregator;
        _messageTypeRegistry = messageTypeRegistry;
        _consumerDispatcher = consumerDispatcher;
        _subscriberDispatcher = subscriberDispatcher;
        _logger = logger;
        // Optional, trailing parameters: keeps existing positional constructor call sites
        // (tests that new-up NymBrokerImpl directly) source-compatible.
        _splitter = splitter ?? new SplitterImpl();
        _compressor = compressor ?? new BrotliCompressor();
    }

    private static FrozenDictionary<string, T> EmptyRegistry<T>()
        => new Dictionary<string, T>().ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);

    private static FrozenDictionary<string, T> WithItem<T>(FrozenDictionary<string, T> registry, string key, T value)
        => new Dictionary<string, T>(registry, registry.Comparer) { [key] = value }.ToFrozenDictionary(registry.Comparer);

    // --- Configuration (called during startup before StartAsync) ---

    public void AddEndpoint(string name, IEndPoint endpoint)
    {
        _endpoints = WithItem(_endpoints, name, endpoint);

        if (endpoint is not IDeferredEndPoint && endpoint.Mode != EndpointMode.ReadOnly)
            _logger.LogWarning(
                "Endpoint '{Name}' cannot defer messages itself: PostDeferredAsync to it holds them in this process's memory, and those still waiting are lost when the process stops or crashes",
                name);
    }

    public void RegisterConsumer(
        Type messageType,
        string serviceKey,
        bool retryOnFailure = true,
        Func<IMessageConsumer, object, IMessageContext, CancellationToken, Task>? dispatcher = null)
    {
        _messageTypeRegistry.Register(messageType);
        if (dispatcher is null)
            _consumerDispatcher.RegisterConsumer(messageType, serviceKey);
        else
            _consumerDispatcher.RegisterConsumerWithDispatcher(messageType, serviceKey, dispatcher);
        if (!retryOnFailure)
            ImmutableInterlocked.Update(ref _noRetryMessageTypes, types => types.Add(messageType));
    }

    public void AddTopic(TopicContext topic)
    {
        _messageTypeRegistry.Register(topic.MessageType);
        _topics = _topics.Add(topic);
    }

    public INymBroker AddFilter(IMessageFilter filter)
    {
        _filters = _filters.Add(filter);
        return this;
    }

    public INymBroker AddInputTransformer(IInputTransformer transformer, string? endpoint = null)
    {
        if (endpoint == null)
            _globalInputTransformer = transformer;
        else
            _endpointTransformers = WithItem(_endpointTransformers, endpoint, transformer);
        return this;
    }

    /// <summary>Transforms every message sent to <paramref name="endpoint"/> into the destination's format (#83).</summary>
    public INymBroker AddOutputTransformer(IOutputTransformer transformer, string endpoint)
    {
        ArgumentNullException.ThrowIfNull(transformer);
        ArgumentException.ThrowIfNullOrEmpty(endpoint);
        _outputTransformers = WithItem(_outputTransformers, endpoint, transformer);
        return this;
    }

    public INymBroker SetDeadLetterEndpoint(string endpointName)
    {
        _deadLetterEndpoint = endpointName;
        return this;
    }

    public INymBroker AddWireTap(string endpointName)
    {
        _wireTapEndpoints = _wireTapEndpoints.Add(endpointName);
        return this;
    }

    public INymBroker SetMaxMessageAge(TimeSpan maxAge)
    {
        _maxMessageAge = maxAge;
        return this;
    }
}
