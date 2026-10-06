using System.Diagnostics.Metrics;

namespace NymBroker.Core.Diagnostics;

public static class NymBrokerDiagnostics
{
    public const string InstrumentationName = "NymBroker";

    private static readonly Meter Meter = new(InstrumentationName);

    internal static readonly Counter<long> MessagesReceived =
        Meter.CreateCounter<long>("nymbroker.messages.received", "{message}");

    internal static readonly Counter<long> MessagesFailed =
        Meter.CreateCounter<long>("nymbroker.messages.failed", "{message}");

    internal static readonly Counter<long> MessagesDeadLettered =
        Meter.CreateCounter<long>("nymbroker.messages.dead_lettered", "{message}");

    // Tags are bounded to config-time names and message types; never add message or payload identifiers.
    internal static readonly Counter<long> MessagesRouted =
        Meter.CreateCounter<long>("nymbroker.messages.routed", "{message}");

    internal static readonly Counter<long> MessagesConsumed =
        Meter.CreateCounter<long>("nymbroker.messages.consumed", "{message}");

    /// <summary>Duplicates dropped by the idempotent receiver; tag <c>source</c>.</summary>
    internal static readonly Counter<long> MessagesDuplicate =
        Meter.CreateCounter<long>("nymbroker.messages.duplicates", "{message}",
            "Duplicate messages dropped by the idempotent receiver.");

    internal static readonly Histogram<double> ProcessingDuration =
        Meter.CreateHistogram<double>("nymbroker.message.processing.duration", "ms");

    // Health checks (recorded on every INymBroker.CheckHealthAsync call, e.g. each /health probe).
    // Tag values are bounded: status / reason are fixed sets, endpoint names are config-time names.

    /// <summary>One per check; tag <c>status</c> = healthy | degraded | unhealthy.</summary>
    internal static readonly Counter<long> HealthChecks =
        Meter.CreateCounter<long>("nymbroker.health.checks", "{check}",
            "Broker health checks, by aggregate status.");

    /// <summary>One per unhealthy endpoint per check; tags <c>endpoint</c>, <c>critical</c>, <c>reason</c> = unhealthy | timeout | error.</summary>
    internal static readonly Counter<long> HealthEndpointFailures =
        Meter.CreateCounter<long>("nymbroker.health.endpoint.failures", "{check}",
            "Endpoints reported unhealthy by a broker health check.");

    /// <summary>Aggregate status of the latest check: 0 = healthy, 1 = degraded, 2 = unhealthy.</summary>
    internal static readonly Gauge<int> HealthStatus =
        Meter.CreateGauge<int>("nymbroker.health.status", null,
            "Latest broker health status: 0 healthy, 1 degraded, 2 unhealthy.");

    /// <summary>Per endpoint, from the latest check: 1 = healthy, 0 = not; tags <c>endpoint</c>, <c>critical</c>.</summary>
    internal static readonly Gauge<int> HealthEndpointHealthy =
        Meter.CreateGauge<int>("nymbroker.health.endpoint.healthy", null,
            "Latest health of each endpoint: 1 healthy, 0 unhealthy.");
}
