namespace NymBroker.Core.Resilience;

/// <summary>Thrown by <see cref="CircuitBreaker.ExecuteAsync(Func{CancellationToken, ValueTask}, CancellationToken)"/> when the circuit refuses the call.</summary>
public sealed class BrokenCircuitException : Exception
{
    public BrokenCircuitException() : this("The circuit is open.") { }

    public BrokenCircuitException(string message) : base(message) { }

    public BrokenCircuitException(string message, Exception innerException) : base(message, innerException) { }

    public BrokenCircuitException(string message, TimeSpan? retryAfter) : base(message) => RetryAfter = retryAfter;

    /// <summary>Time until the circuit lets a trial call through; null while a trial call is already running.</summary>
    public TimeSpan? RetryAfter { get; }
}
