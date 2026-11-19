using System.Diagnostics.Metrics;

namespace NymBroker.Core.Resilience;

/// <summary>The instruments of the <c>NymBroker.Resilience</c> meter, shared by the retry policy, circuit breaker and timeouts.</summary>
internal static class ResilienceDiagnostics
{
    private static readonly Meter Meter = new(RetryPolicy.InstrumentationName);

    internal static readonly Counter<long> RetryAttempts =
        Meter.CreateCounter<long>("nymbroker.retries", "{retry}");

    /// <summary>A circuit changed state; tags <c>circuit</c>, <c>state</c>.</summary>
    internal static readonly Counter<long> CircuitStateChanges =
        Meter.CreateCounter<long>("nymbroker.circuit_breaker.state_changes", "{change}",
            "Circuit breaker state changes.");

    /// <summary>A call refused by an open circuit (<see cref="CircuitBreaker.ExecuteAsync(Func{CancellationToken, ValueTask}, CancellationToken)"/>); tag <c>circuit</c>.</summary>
    internal static readonly Counter<long> CircuitRejected =
        Meter.CreateCounter<long>("nymbroker.circuit_breaker.rejected", "{call}",
            "Calls refused because the circuit was open.");

    /// <summary>A message held by the broker until its consumer's circuit let it through; tag <c>circuit</c>.</summary>
    internal static readonly Counter<long> CircuitHeld =
        Meter.CreateCounter<long>("nymbroker.circuit_breaker.held", "{message}",
            "Messages the broker held because their consumer's circuit was open.");

    /// <summary>A consumer that did not finish within its timeout; tag <c>consumer</c>.</summary>
    internal static readonly Counter<long> ConsumerTimeouts =
        Meter.CreateCounter<long>("nymbroker.consumer.timeouts", "{message}",
            "Consumers that did not finish within their configured timeout.");
}
