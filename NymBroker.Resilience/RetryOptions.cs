namespace NymBroker.Resilience;

public sealed class RetryOptions
{
    /// <summary>Retries after the first attempt (total executions = 1 + this). Must be ≥ 1.</summary>
    public int MaxRetryAttempts { get; init; } = 3;

    /// <summary>Base delay between attempts.</summary>
    public TimeSpan Delay { get; init; } = TimeSpan.FromSeconds(2);

    /// <summary>Upper bound for any single delay. <c>null</c> caps at 24 hours.</summary>
    public TimeSpan? MaxDelay { get; init; }

    public RetryBackoffType BackoffType { get; init; } = RetryBackoffType.Constant;

    /// <summary>
    /// Randomizes delays: ±25% for <see cref="RetryBackoffType.Constant"/>, decorrelated jitter
    /// for <see cref="RetryBackoffType.Exponential"/>.
    /// </summary>
    public bool UseJitter { get; init; }

    /// <summary>Decides whether an exception is retried. Default: anything but <see cref="OperationCanceledException"/>.</summary>
    public Func<Exception, bool> ShouldHandle { get; init; } = static ex => ex is not OperationCanceledException;

    /// <summary>Invoked before each retry delay.</summary>
    public Func<RetryAttempt, ValueTask>? OnRetry { get; init; }

    /// <summary>Clock used for delays — override in tests.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
}
