using NymBroker.Core.Description;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Message;
using NymBroker.Core.PubSub;
using NymBroker.Core.Route;

namespace NymBroker.Core.Impl;

internal sealed partial class NymBrokerImpl
{
    /// <summary>
    /// Builds the snapshot from the registries as they are now. Every registry is an immutable collection replaced on
    /// write, so this needs no lock and never sees a half-made change.
    /// </summary>
    public BrokerDescription Describe() => new()
    {
        Started = _started,
        Endpoints = [.. _endpoints.OrderBy(e => e.Key, StringComparer.OrdinalIgnoreCase).Select(e => DescribeEndpoint(e.Key, e.Value))],
        Routes = [.. _routes.Select(DescribeRoute)],
        Topics = [.. _topics.Select(DescribeTopic)],
        Consumers = [.. _consumerDispatcher.Registrations.Select(DescribeConsumer).OrderBy(c => c.MessageType, StringComparer.Ordinal)],
        ScheduledActions = DescribeScheduledActions(DateTimeOffset.Now),
        Features = DescribeFeatures()
    };

    private EndpointDescription DescribeEndpoint(string name, IEndPoint endpoint) => new()
    {
        SendRateLimit = SendThrottleFor(name)?.RateLimit?.ToString(),
        SendConcurrencyLimit = SendThrottleFor(name)?.ConcurrencyLimit,
        Name = name,
        Kind = EndpointKind(endpoint.GetType()),
        Mode = endpoint.Mode,
        Listens = endpoint is IEndPointEventDriven && endpoint.Mode != EndpointMode.WriteOnly,
        UsesNativeDeadLetter = endpoint is IEndPointEventDriven { UsesNativeDeadLetter: true },
        CanDefer = endpoint is IDeferredEndPoint,
        Inspectable = endpoint is Endpoint.Queue.IQueueInspector,
        Replayable = endpoint is Endpoint.Queue.IQueueReplayer,
        FailedRemovable = endpoint is Endpoint.Queue.IQueueFailedRemover,
        IsCritical = _healthCheckOptions.IsCritical(name),
        HasInputTransformer = _endpointTransformers.ContainsKey(name),
        HasOutputTransformer = _outputTransformers.ContainsKey(name)
    };

    /// <summary><c>SqliteEndPoint</c> → <c>Sqlite</c>, <c>MemoryQueueEndPoint</c> → <c>Memory</c>; other names are kept.</summary>
    internal static string EndpointKind(Type endpointType)
    {
        var name = endpointType.Name;
        var backtick = name.IndexOf('`', StringComparison.Ordinal);
        if (backtick > 0) name = name[..backtick];
        foreach (var suffix in (ReadOnlySpan<string>)["QueueEndPoint", "EndPoint", "Endpoint"])
        {
            if (name.Length > suffix.Length && name.EndsWith(suffix, StringComparison.Ordinal))
                return name[..^suffix.Length];
        }
        return name;
    }

    private static RouteDescription DescribeRoute(RouteContext route) => new()
    {
        MessageType = TypeName(route.MessageType),
        Destination = route.DestinationEndpoint,
        From = route.SourceEndpoint,
        NotFrom = route.ExcludedSourceEndpoint,
        Condition = route.Condition is null ? null : RouteConditions.Describe(route.Condition)
    };

    private static TopicDescription DescribeTopic(TopicContext topic) => new()
    {
        Name = topic.TopicName,
        MessageType = TypeName(topic.MessageType),
        Condition = topic.Condition is null ? null : RouteConditions.Describe(topic.Condition),
        SubscriberEndpoints = [.. topic.SubscriberEndpoints],
        Subscribers = [.. topic.SubscriberDispatchers.Select(s => s.SubscriberType.Name)]
    };

    private ConsumerDescription DescribeConsumer((Type MessageType, string ServiceKey) registration)
    {
        var resilience = ResilienceFor(registration.MessageType);
        return new()
        {
            MessageType = TypeName(registration.MessageType),
            Consumer = registration.ServiceKey,
            RetryOnFailure = !_noRetryMessageTypes.Contains(registration.MessageType),
            Timeout = resilience?.Timeout,
            CircuitState = resilience?.Breaker?.State,
            RateLimit = resilience?.Throttle?.RateLimit?.ToString(),
            ConcurrencyLimit = resilience?.Throttle?.ConcurrencyLimit
        };
    }

    private List<ScheduledActionDescription> DescribeScheduledActions(DateTimeOffset now)
    {
        // Read under the lock, computed outside it: the list is immutable.
        System.Collections.Immutable.ImmutableList<ScheduledActionInfo> infos;
        lock (_scheduleLock) infos = _scheduledActionInfos;
        return [.. infos.Select(info => new ScheduledActionDescription
        {
            Name = info.Name,
            Schedule = info.Schedule,
            MissedRuns = info.MissedRuns,
            NextRun = info.Timing?.Next(now)
        })];
    }

    private BrokerFeatures DescribeFeatures() => new()
    {
        DeadLetterEndpoint = _deadLetterEndpoint,
        WireTaps = [.. _wireTapEndpoints],
        MaxMessageAge = _maxMessageAge,
        IdempotencyStore = _idempotencyStore?.GetType().Name,
        Compressor = _compressor.Name,
        Filters = _filters.Length,
        HasGlobalInputTransformer = _globalInputTransformer is not null
    };

    private static string TypeName(Type messageType) => messageType == typeof(IAnyMessage) ? "*" : MessageTypeName.Get(messageType);
}
