using NymBroker.Core.Resilience;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Scheduling;

namespace NymBroker.Core.Description;

/// <summary>
/// A read-only snapshot of how a broker is configured: its endpoints, routes, topics, consumers and scheduled actions.
/// Returned by <see cref="INymBroker.Describe"/> for dashboards, diagnostics and tests. Building it does no I/O.
/// </summary>
/// <remarks>Message types are given by their wire name (<see cref="Message.MessageTypeName"/>); <c>*</c> stands for any message type.</remarks>
public sealed record BrokerDescription
{
    /// <summary>Whether the broker has started and not been stopped since.</summary>
    public required bool Started { get; init; }
    public required IReadOnlyList<EndpointDescription> Endpoints { get; init; }
    public required IReadOnlyList<RouteDescription> Routes { get; init; }
    public required IReadOnlyList<TopicDescription> Topics { get; init; }
    public required IReadOnlyList<ConsumerDescription> Consumers { get; init; }
    public required IReadOnlyList<ScheduledActionDescription> ScheduledActions { get; init; }
    public required BrokerFeatures Features { get; init; }
}

/// <summary>One registered endpoint and what it can do.</summary>
public sealed record EndpointDescription
{
    public required string Name { get; init; }

    /// <summary>The transport, from the endpoint's class name: <c>Sqlite</c>, <c>RabbitMq</c>, <c>Memory</c>, … (<c>SqliteEndPoint</c> → <c>Sqlite</c>).</summary>
    public required string Kind { get; init; }

    public required EndpointMode Mode { get; init; }

    /// <summary>The broker receives from it: it is event-driven and not <see cref="EndpointMode.WriteOnly"/>.</summary>
    public required bool Listens { get; init; }

    /// <summary>The transport dead-letters messages itself (see <see cref="IEndPointEventDriven.UsesNativeDeadLetter"/>).</summary>
    public required bool UsesNativeDeadLetter { get; init; }

    /// <summary>It implements <see cref="IDeferredEndPoint"/>, so deferred messages wait in the transport rather than in the broker.</summary>
    public required bool CanDefer { get; init; }

    /// <summary>It offers an <see cref="Endpoint.Queue.IQueueInspector"/> (<see cref="INymBroker.GetQueueInspector"/> returns one).</summary>
    public required bool Inspectable { get; init; }

    /// <summary>Health checks count it as critical (see <see cref="Endpoint.HealthCheck.BrokerHealthCheckOptions.NonCritical"/>).</summary>
    public required bool IsCritical { get; init; }

    public required bool HasInputTransformer { get; init; }
    public required bool HasOutputTransformer { get; init; }

    /// <summary>The send rate limit set with <c>LimitSendsTo</c>, as text; null when there is none.</summary>
    public string? SendRateLimit { get; init; }

    /// <summary>The send concurrency limit set with <c>LimitSendsTo</c>; null when there is none.</summary>
    public int? SendConcurrencyLimit { get; init; }
}

/// <summary>A route: messages of <see cref="MessageType"/> that meet the conditions are forwarded to <see cref="Destination"/>.</summary>
public sealed record RouteDescription
{
    public required string MessageType { get; init; }
    public required string Destination { get; init; }

    /// <summary>Only messages received on this endpoint (<c>WhenFrom</c>), or null.</summary>
    public string? From { get; init; }

    /// <summary>Messages received on any endpoint but this one (<c>WhenNotFrom</c>), or null.</summary>
    public string? NotFrom { get; init; }

    /// <summary>The content condition as text, e.g. <c>predicate and older than 00:05:00</c>, or null when there is none.</summary>
    public string? Condition { get; init; }
}

/// <summary>A topic and who receives what is published to it.</summary>
public sealed record TopicDescription
{
    public required string Name { get; init; }
    public required string MessageType { get; init; }
    public string? Condition { get; init; }
    public required IReadOnlyList<string> SubscriberEndpoints { get; init; }

    /// <summary>The <c>ISubscribe&lt;T&gt;</c> classes, by type name.</summary>
    public required IReadOnlyList<string> Subscribers { get; init; }
}

/// <summary>The consumer class that handles a message type.</summary>
public sealed record ConsumerDescription
{
    public required string MessageType { get; init; }

    /// <summary>The consumer class, by type name (also its DI key).</summary>
    public required string Consumer { get; init; }

    /// <summary>False when it was registered with <c>RetryOnFailure = false</c>.</summary>
    public required bool RetryOnFailure { get; init; }

    /// <summary>The consumer's <c>ConsumerOptions.Timeout</c>; null when it has none.</summary>
    public TimeSpan? Timeout { get; init; }

    /// <summary>The state of the consumer's circuit breaker now; null when it has none.</summary>
    public CircuitState? CircuitState { get; init; }

    /// <summary>The consumer's <c>ConsumerOptions.RateLimit</c> as text (e.g. <c>50/1s, burst 50</c>); null when it has none.</summary>
    public string? RateLimit { get; init; }

    /// <summary>The consumer's <c>ConsumerOptions.ConcurrencyLimit</c>; null when it has none.</summary>
    public int? ConcurrencyLimit { get; init; }
}

/// <summary>A scheduled action.</summary>
public sealed record ScheduledActionDescription
{
    /// <summary>The name of a named action (claimed in the schedule store, runs once per cluster), or null.</summary>
    public string? Name { get; init; }

    /// <summary>A cron expression, or an interval such as <c>every 00:05:00</c>.</summary>
    public required string Schedule { get; init; }

    /// <summary>For a named action, what happens to an occurrence missed while the broker was down; otherwise null.</summary>
    public MissedRunPolicy? MissedRuns { get; init; }

    /// <summary>
    /// The next occurrence after the snapshot was taken, for cron and named actions. Null for an unnamed interval
    /// action, whose occurrences count from the broker's start.
    /// </summary>
    public DateTimeOffset? NextRun { get; init; }
}

/// <summary>Broker-wide settings that change what happens to every message.</summary>
public sealed record BrokerFeatures
{
    public string? DeadLetterEndpoint { get; init; }
    public required IReadOnlyList<string> WireTaps { get; init; }

    /// <summary>Messages older than this are dead-lettered as expired (<c>DiscardMessagesOlderThan</c>), or null.</summary>
    public TimeSpan? MaxMessageAge { get; init; }

    /// <summary>The idempotency store's type name, or null when duplicates are not checked.</summary>
    public string? IdempotencyStore { get; init; }

    /// <summary>The codec split messages are compressed with (<see cref="Splitter.ICompressor.Name"/>).</summary>
    public required string Compressor { get; init; }

    public required int Filters { get; init; }
    public required bool HasGlobalInputTransformer { get; init; }
}
