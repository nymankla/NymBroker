namespace NymBroker.Core.Resilience;

/// <summary>Options of a <see cref="CircuitBreaker"/>. Validated by its constructor.</summary>
public sealed class CircuitBreakerOptions
{
    /// <summary>Share of failed calls in the sampling window that opens the circuit, in (0, 1]. Default 0.5.</summary>
    public double FailureRatio { get; init; } = 0.5;

    /// <summary>Calls the sampling window must hold before the ratio can open the circuit. At least 1. Default 10.</summary>
    public int MinimumThroughput { get; init; } = 10;

    /// <summary>Length of the rolling window the ratio is measured over. At least 10 ms. Default 30 s.</summary>
    public TimeSpan SamplingDuration { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>How long the circuit stays open before it lets a trial call through. Above zero, at most 1 day. Default 30 s.</summary>
    public TimeSpan BreakDuration { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Decides whether an exception counts as a failure; others are not counted. Default: every exception, including an
    /// <see cref="OperationCanceledException"/> the call raised itself (e.g. an <c>HttpClient</c> timeout). Cancellation of
    /// the caller's own token is never counted.
    /// </summary>
    public Func<Exception, bool> ShouldHandle { get; init; } = static _ => true;

    /// <summary>Clock for the window and the break — override in tests.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
}
