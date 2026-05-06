using System.Collections.Immutable;
using System.Text;
using System.Text.Json;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Filter;
using NymBroker.Core.Message;
using NymBroker.Core.PubSub;
using NymBroker.Core.Route;
using NymBroker.Core.Serialize;
using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Impl;

public sealed class NymBrokerImpl : INymBroker
{
    private readonly MessageSerializerJson _serializer;
    private readonly IAggregator _aggregator;
    private readonly MessageTypeRegistry _messageTypeRegistry;
    private readonly ConsumerDispatcher _consumerDispatcher;
    private readonly SubscriberDispatcher _subscriberDispatcher;
    private readonly ILogger<NymBrokerImpl> _logger;
    private readonly SemaphoreSlim _lifecycleLock = new(1, 1);
    private ImmutableList<Func<CancellationToken, Task<ScheduledActionHandle>>> _scheduledActions = ImmutableList<Func<CancellationToken, Task<ScheduledActionHandle>>>.Empty;
    private ImmutableList<ScheduledActionHandle> _activeScheduledActions = ImmutableList<ScheduledActionHandle>.Empty;
    private bool _started;

    // Thread-safe via immutable replacement — writes are infrequent (config-time only).
    private ImmutableList<RouteContext> _routes = ImmutableList<RouteContext>.Empty;
    private ImmutableList<IMessageFilter> _filters = ImmutableList<IMessageFilter>.Empty;
    private ImmutableList<TopicContext> _topics = ImmutableList<TopicContext>.Empty;

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

    public void AddEndpoint(IEndPoint endpoint) => _endpoints = _endpoints.SetItem(endpoint.Name, endpoint);

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

    // --- INymBroker ---

    public INymBroker AddFilter(IMessageFilter filter)
    {
        _filters = _filters.Add(filter);
        return this;
    }

    public INymBroker AddScheduledAction(TimeSpan timeSpan, Action action)
    {
        ArgumentNullException.ThrowIfNull(action);
        _scheduledActions = _scheduledActions.Add(ct => StartIntervalScheduledActionAsync(timeSpan, timeSpan, action, ct));
        return this;
    }

    public INymBroker AddScheduledAction<T1>(TimeSpan timeSpan, Action<T1> action, T1 param1)
    {
        ArgumentNullException.ThrowIfNull(action);
        _scheduledActions = _scheduledActions.Add(ct => StartIntervalScheduledActionAsync(timeSpan, timeSpan, () => action(param1), ct));
        return this;
    }

    public INymBroker AddScheduledAction<T1, T2>(TimeSpan timeSpan, Action<T1, T2> action, T1 param1, T2 param2)
    {
        ArgumentNullException.ThrowIfNull(action);
        _scheduledActions = _scheduledActions.Add(ct => StartIntervalScheduledActionAsync(timeSpan, timeSpan, () => action(param1, param2), ct));
        return this;
    }

    public INymBroker AddScheduledAction<T1>(string expression, Action<T1> action, T1 param1)
    {
        ArgumentException.ThrowIfNullOrEmpty(expression);
        ArgumentNullException.ThrowIfNull(action);

        var cronAction = new CronScheduledAction<T1>(expression, action, param1);
        _logger.LogInformation("Scheduled cron action set up, first occurrence at {NextOccurrence}", cronAction.NextOccurrence(DateTimeOffset.Now));
        _scheduledActions = _scheduledActions.Add(ct => StartCronScheduledActionAsync(cronAction, ct));
        return this;
    }

    public IRouteBuilder<T> Route<T>() where T : class
    {
        _messageTypeRegistry.Register(typeof(T));
        return new RouteBuilder<T>(ctx =>
        {
            _messageTypeRegistry.Register(ctx.MessageType);
            _routes = _routes.Add(ctx);
        });
    }

    public IRouteBuilder<IAnyMessage> Route()
        => Route<IAnyMessage>();

    public RouteContext Route(IRouteBuilder routeBuilder)
    {
        ArgumentNullException.ThrowIfNull(routeBuilder);
        var routeContext = routeBuilder.Build();
        _messageTypeRegistry.Register(routeContext.MessageType);
        _routes = _routes.Add(routeContext);
        return routeContext;
    }

    public IRouteBuilder<IAnyMessage> Route(Func<RouteContext> routeContextFactory)
    {
        ArgumentNullException.ThrowIfNull(routeContextFactory);
        return new RouteBuilder<IAnyMessage>(ctx =>
        {
            _messageTypeRegistry.Register(ctx.MessageType);
            _routes = _routes.Add(ctx);
        }, routeContextFactory);
    }

    public async Task PostAsync<T>(string endpointName, T message, CancellationToken ct = default) where T : class
    {
        var context = new MessageContext<T>
        {
            Message = message,
            Address = EndpointAddress.Create(endpointName)
        };

        using var stream = _serializer.Serialize(context);
        await PostToEndpointAsync(endpointName, stream, ct);
    }

    public async Task PostAsync(string endpointName, Stream messageStream, CancellationToken ct = default)
        => await PostToEndpointAsync(endpointName, messageStream, ct);

    public async Task PublishAsync<T>(T message, CancellationToken ct = default) where T : class
    {
        var context = new MessageContext<T> { Message = message };
        using var stream = _serializer.Serialize(context);
        stream.Position = 0;
        using var reader = new StreamReader(stream, Encoding.UTF8, leaveOpen: true);
        var raw = await reader.ReadToEndAsync(ct);
        await ProcessAsync(raw, null, ct);
    }

    public async Task PublishAsync<T>(string topicName, T message, CancellationToken ct = default) where T : class
    {
        var topic = _topics.FirstOrDefault(t => string.Equals(t.TopicName, topicName, StringComparison.OrdinalIgnoreCase));
        if (topic == null)
        {
            _logger.LogWarning("No topic registered with name '{TopicName}'", topicName);
            return;
        }

        var context = new MessageContext<T> { Message = message };
        await FanOutTopicAsync(topic, message, context, ct);
    }

    public async Task ProcessAsync(string raw, string? sourceEndpoint = null, CancellationToken ct = default)
    {
        IMessageContext context;
        try { context = _serializer.Deserialize(raw); }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to deserialize message from {Source}", sourceEndpoint);
            return;
        }

        if (context.Address == null) context.Address = new EndpointAddress();
        context.Address.From = sourceEndpoint;

        // Apply filters.
        foreach (var filter in _filters)
        {
            context = filter.Filter(context)!;
            if (context == null) return;
        }

        if (context is not RawMessageContext raw2)
        {
            _logger.LogWarning("Unexpected context type: {Type}", context.GetType().Name);
            return;
        }

        // Resolve CLR type.
        var messageType = _messageTypeRegistry.Resolve(raw2.MessageType);

        // Aggregator: collect SplitMessage parts before further processing.
        if (messageType == typeof(SplitMessage))
        {
            var split = MessageSerializerJson.DeserializeMessage<SplitMessage>(raw2);
            if (split == null) return;

            var reassembled = await _aggregator.AddAsync(split, context, ct);
            if (reassembled == null) return;

            var reassembledJson = Encoding.UTF8.GetString(reassembled);
            await ProcessAsync(reassembledJson, sourceEndpoint, ct);
            return;
        }

        var msgElement = raw2.RawMessage;

        // Route to destination endpoints.
        var wasRouted = false;
        foreach (var route in _routes)
        {
            if (!route.Evaluate(messageType ?? typeof(IAnyMessage), context, msgElement)) continue;

            if (!_endpoints.TryGetValue(route.DestinationEndpoint, out var destEndpoint))
            {
                _logger.LogWarning("Route references unknown endpoint '{Dest}'", route.DestinationEndpoint);
                continue;
            }

            using var stream = _serializer.Serialize(context);
            await destEndpoint.PostAsync(stream, ct);
            _logger.LogInformation(
                "Routed message type {MessageType} from {Source} to {Destination}",
                raw2.MessageType ?? messageType?.FullName ?? typeof(IAnyMessage).FullName,
                context.Address?.From,
                route.DestinationEndpoint);
            wasRouted = true;
        }

        // Topic fan-out (pub/sub) — evaluated regardless of whether a route also matched.
        var wasTopicFanOut = false;
        object? deserializedMessage = null;
        foreach (var topic in _topics)
        {
            if (!topic.Evaluate(messageType ?? typeof(IAnyMessage), context, msgElement)) continue;
            wasTopicFanOut = true;

            // Lazily deserialize once — reused across multiple matching topics.
            if (topic.SubscriberDispatchers.Count > 0 && messageType != null && deserializedMessage == null)
                deserializedMessage = MessageSerializerJson.DeserializeMessageObject(raw2, messageType);

            await FanOutTopicAsync(topic, deserializedMessage, context, ct);
        }

        if (wasRouted || wasTopicFanOut)
            return;

        if (messageType == null)
        {
            _logger.LogWarning("No consumer or route for unresolved message type '{MessageType}' from {Source}", raw2.MessageType, sourceEndpoint);
            return;
        }

        var message = MessageSerializerJson.DeserializeMessageObject(raw2, messageType);
        if (message != null)
            await _consumerDispatcher.DispatchAsync(messageType, message, context, ct);
    }

    public async Task StartAsync(CancellationToken ct = default)
    {
        await _lifecycleLock.WaitAsync(ct);
        try
        {
            if (_started)
                return;

            var startedScheduledActions = ImmutableList<ScheduledActionHandle>.Empty;
            try
            {
                foreach (var scheduledAction in _scheduledActions)
                    startedScheduledActions = startedScheduledActions.Add(await scheduledAction(ct));

                foreach (var endpoint in _endpoints.Values)
                {
                    if (endpoint is IEndPointEventDriven ed)
                    {
                        var name = endpoint.Name;
                        await ed.StartListeningAsync(
                            async (raw, token) => await ProcessAsync(raw, name, token),
                            ct);
                        _logger.LogInformation("Started listening on endpoint '{Name}'", name);
                    }
                }

                _activeScheduledActions = startedScheduledActions;
                _started = true;
                _logger.LogInformation("Broker started");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Broker failed to start — rolling back scheduled actions");
                foreach (var scheduledAction in startedScheduledActions)
                    await scheduledAction.DisposeAsync();

                throw;
            }
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    public async Task StopAsync(CancellationToken ct = default)
    {
        await _lifecycleLock.WaitAsync(ct);
        try
        {
            if (!_started)
                return;

            foreach (var scheduledAction in _activeScheduledActions)
                await scheduledAction.DisposeAsync();

            _activeScheduledActions = ImmutableList<ScheduledActionHandle>.Empty;

            foreach (var endpoint in _endpoints.Values.OfType<IEndPointEventDriven>())
                await endpoint.StopListeningAsync();

            _started = false;
            _logger.LogInformation("Broker stopped");
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    // --- Helpers ---

    private async Task FanOutTopicAsync(TopicContext topic, object? message, IMessageContext context, CancellationToken ct)
    {
        foreach (var endpointName in topic.SubscriberEndpoints)
        {
            if (!_endpoints.TryGetValue(endpointName, out var endpoint))
            {
                _logger.LogWarning("Topic '{Topic}' references unknown endpoint '{Endpoint}'", topic.TopicName, endpointName);
                continue;
            }
            try
            {
                using var stream = _serializer.Serialize(context);
                await endpoint.PostAsync(stream, ct);
                _logger.LogInformation("Topic '{Topic}' delivered to endpoint '{Endpoint}'", topic.TopicName, endpointName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Topic '{Topic}' failed to deliver to endpoint '{Endpoint}'", topic.TopicName, endpointName);
            }
        }

        if (message != null && topic.SubscriberDispatchers.Count > 0)
            await _subscriberDispatcher.DispatchAsync(topic.SubscriberDispatchers, message, context, ct);
    }

    private async Task PostToEndpointAsync(string name, Stream stream, CancellationToken ct)
    {
        if (!_endpoints.TryGetValue(name, out var endpoint))
            throw new InvalidOperationException($"No endpoint registered with name '{name}'.");
        await endpoint.PostAsync(stream, ct);
    }

    private Task<ScheduledActionHandle> StartIntervalScheduledActionAsync(TimeSpan initialDelay, TimeSpan interval, Action action, CancellationToken ct)
    {
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var task = Task.Run(async () =>
        {
            await Task.Delay(initialDelay, linkedCts.Token);
            using var timer = new PeriodicTimer(interval);

            do
            {
                action();
            }
            while (await timer.WaitForNextTickAsync(linkedCts.Token));
        }, linkedCts.Token);

        return Task.FromResult(new ScheduledActionHandle(linkedCts, task));
    }

    private Task<ScheduledActionHandle> StartCronScheduledActionAsync<T>(CronScheduledAction<T> cronAction, CancellationToken ct)
    {
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var task = Task.Run(async () =>
        {
            while (!linkedCts.Token.IsCancellationRequested)
            {
                var nextOccurrence = cronAction.NextOccurrence(DateTimeOffset.Now);
                if (nextOccurrence == null)
                    break;

                var delay = nextOccurrence.Value - DateTimeOffset.Now;
                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, linkedCts.Token);

                cronAction.Invoke();
            }
        }, linkedCts.Token);

        return Task.FromResult(new ScheduledActionHandle(linkedCts, task));
    }
}
