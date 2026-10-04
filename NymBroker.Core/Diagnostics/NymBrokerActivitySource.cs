using System.Diagnostics;

namespace NymBroker.Core.Diagnostics;

public static class NymBrokerActivitySource
{
    public const string InstrumentationName = NymBrokerDiagnostics.InstrumentationName;

    public static ActivitySource Source { get; } = new(InstrumentationName);
}
