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

    internal static readonly Histogram<double> ProcessingDuration =
        Meter.CreateHistogram<double>("nymbroker.message.processing.duration", "ms");
}
