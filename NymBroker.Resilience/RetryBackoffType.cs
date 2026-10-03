namespace NymBroker.Resilience;

public enum RetryBackoffType
{
    /// <summary>Every retry waits <see cref="RetryOptions.Delay"/>.</summary>
    Constant,

    /// <summary>Retry <c>n</c> (0-based) waits <c>Delay * 2^n</c>.</summary>
    Exponential
}
