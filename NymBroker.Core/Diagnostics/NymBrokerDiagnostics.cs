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

    internal static readonly Histogram<double> ProcessingDuration =
        Meter.CreateHistogram<double>("nymbroker.message.processing.duration", "ms");
}
