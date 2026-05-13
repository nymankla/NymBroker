using System.Collections.Immutable;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Filter;
using NymBroker.Core.Transform;
using NymBroker.Core.PubSub;
using NymBroker.Core.Route;
using NymBroker.Core.Serialize;
using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Impl;

public sealed partial class NymBrokerImpl : INymBroker
{
    private readonly MessageSerializerJson _serializer;
    private readonly IAggregator _aggregator;
    private readonly MessageTypeRegistry _messageTypeRegistry;
    private readonly ConsumerDispatcher _consumerDispatcher;
    private readonly SubscriberDispatcher _subscriberDispatcher;
    private readonly ILogger<NymBrokerImpl> _logger;
    private readonly SemaphoreSlim _lifecycleLock = new(1, 1);
    private readonly TaskCompletionSource _startGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private ImmutableList<Func<CancellationToken, Task<ScheduledActionHandle>>> _scheduledActions = ImmutableList<Func<CancellationToken, Task<ScheduledActionHandle>>>.Empty;
    private ImmutableList<ScheduledActionHandle> _activeScheduledActions = ImmutableList<ScheduledActionHandle>.Empty;
    private volatile bool _startInitiated;
    private bool _started;

    // Thread-safe via immutable replacement — writes are infrequent (config-time only).
    private ImmutableList<RouteContext> _routes = ImmutableList<RouteContext>.Empty;
    private ImmutableList<IMessageFilter> _filters = ImmutableList<IMessageFilter>.Empty;
    private ImmutableList<TopicContext> _topics = ImmutableList<TopicContext>.Empty;
    private ImmutableDictionary<string, IInputTransformer> _endpointTransformers =
        ImmutableDictionary.Create<string, IInputTransformer>(StringComparer.OrdinalIgnoreCase);
    private IInputTransformer? _globalInputTransformer;
    private ImmutableList<string> _wireTapEndpoints = ImmutableList<string>.Empty;
    private string? _deadLetterEndpoint;
    private TimeSpan? _maxMessageAge;

    // Endpoint registry — replaced immutably during configuration, then read-only during runtime.
    private ImmutableDictionary<string, IEndPoint> _endpoints = ImmutableDictionary.Create<string, IEndPoint>(StringComparer.OrdinalIgnoreCase);

    public NymBrokerImpl(
        MessageSerializerJson serializer,
        IAggregator aggregator,
        MessageTypeRegistry messageTypeRegistry,
        ConsumerDispatcher consumerDispatcher,
        SubscriberDispatcher subscriberDispatcher,
        ILogger<NymBrokerImpl> logger)
    {
        _serializer = serializer;
        _aggregator = aggregator;
        _messageTypeRegistry = messageTypeRegistry;
        _consumerDispatcher = consumerDispatcher;
        _subscriberDispatcher = subscriberDispatcher;
        _logger = logger;
    }

    // --- Configuration (called during startup before StartAsync) ---

    public void AddEndpoint(string name, IEndPoint endpoint) => _endpoints = _endpoints.SetItem(name, endpoint);

    public void RegisterConsumer(Type messageType, string serviceKey)
    {
        _messageTypeRegistry.Register(messageType);
        _consumerDispatcher.RegisterConsumer(messageType, serviceKey);
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
            _endpointTransformers = _endpointTransformers.SetItem(endpoint, transformer);
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
