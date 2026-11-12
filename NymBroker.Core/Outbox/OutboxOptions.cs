// CA2208: the name reported is the option that is wrong, not a parameter of Validate.
#pragma warning disable CA2208

namespace NymBroker.Core.Outbox;

/// <summary>How the outbox dispatcher reads and sends the outbox table.</summary>
public sealed class OutboxOptions
{
    /// <summary>How long the dispatcher waits before looking again when the outbox had no full batch. Default 1 second.</summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Rows claimed at a time. Default 100.</summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// How long a claim lasts. A dispatcher that dies holding rows leaves them to the next claim after this; a batch still
    /// being sent then is sent again, so keep it above the time a batch takes. Whole seconds, default 1 minute.
    /// </summary>
    public TimeSpan LeaseTimeout { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>Attempts before a message is marked failed and left in the table. Default 10.</summary>
    public int MaxAttempts { get; set; } = 10;

    /// <summary>The wait after the first failed send, doubled after each further one up to <see cref="MaxRetryDelay"/>. Default 1 second.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>The longest wait between two attempts. Default 5 minutes.</summary>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Throws <see cref="ArgumentException"/> if an option is out of range. Runs at registration.</summary>
    public void Validate()
    {
        if (PollInterval < TimeSpan.FromMilliseconds(1) || PollInterval > TimeSpan.FromDays(1))
            throw new ArgumentException("PollInterval must be between 1 millisecond and 1 day.", nameof(PollInterval));
        if (BatchSize < 1)
            throw new ArgumentException("BatchSize must be at least 1.", nameof(BatchSize));
        if (LeaseTimeout < TimeSpan.FromSeconds(1) || LeaseTimeout > TimeSpan.FromDays(1))
            throw new ArgumentException("LeaseTimeout must be between 1 second and 1 day.", nameof(LeaseTimeout));
        if (MaxAttempts < 1)
            throw new ArgumentException("MaxAttempts must be at least 1.", nameof(MaxAttempts));
        if (RetryDelay < TimeSpan.Zero || MaxRetryDelay < RetryDelay || MaxRetryDelay > TimeSpan.FromDays(1))
            throw new ArgumentException("RetryDelay must not be negative, and MaxRetryDelay must be between RetryDelay and 1 day.", nameof(RetryDelay));
    }

    /// <summary>The wait after a failed attempt: <see cref="RetryDelay"/> doubled per earlier attempt, at most <see cref="MaxRetryDelay"/>.</summary>
    internal TimeSpan RetryDelayAfter(int attempt)
    {
        var milliseconds = RetryDelay.TotalMilliseconds * Math.Pow(2, Math.Max(0, attempt - 1));
        return TimeSpan.FromMilliseconds(Math.Min(milliseconds, MaxRetryDelay.TotalMilliseconds));
    }
}
