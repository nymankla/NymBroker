namespace NymBroker.Core.Resilience;

/// <summary>Passed to <see cref="RetryOptions.OnRetry"/> before each retry delay.</summary>
/// <param name="AttemptNumber">0-based number of the attempt that just failed.</param>
/// <param name="Exception">The exception that triggered the retry.</param>
/// <param name="Delay">How long the policy will wait before the next attempt.</param>
public readonly record struct RetryAttempt(int AttemptNumber, Exception Exception, TimeSpan Delay);
