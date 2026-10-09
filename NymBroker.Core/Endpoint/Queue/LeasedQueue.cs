namespace NymBroker.Core.Endpoint.Queue;

/// <summary>
/// The settings <see cref="LeasedQueueListener"/> needs, shared by every queue-table endpoint (SQLite, PostgreSQL,
/// SQL Server). Each endpoint's settings class implements it.
/// </summary>
public interface ILeasedQueueSettings
{
    /// <summary>Messages claimed per round trip.</summary>
    int BatchSize { get; }

    /// <summary>How long to wait after a poll that found no messages. While messages are waiting, batches are claimed back to back.</summary>
    TimeSpan PollInterval { get; }

    /// <summary>How long a claimed message stays locked; afterwards another poller may claim it again.</summary>
    TimeSpan LeaseTimeout { get; }

    /// <summary>Attempts (claims) before a message that keeps failing is marked <see cref="QueueMessageStatus.Failed"/>.</summary>
    int MaxRetryCount { get; }

    /// <summary>
    /// When true, failures are settled in the table (retried until <see cref="MaxRetryCount"/>, dead letters marked
    /// Failed at once); when false, the broker posts failures to its own dead-letter endpoint.
    /// </summary>
    bool UseNativeDeadLetter { get; }
}

/// <summary>Row status in a queue table. The values are stored in the table, so they must not change.</summary>
public enum QueueMessageStatus
{
    Pending = 0,
    InProgress = 1,
    Completed = 2,
    Failed = 3
}

/// <summary>A claimed row.</summary>
/// <param name="QueueId">The row's key; finalize statements use it together with <paramref name="AttemptCount"/>.</param>
/// <param name="AttemptCount">The attempt count after this claim. Finalize statements must only update a row still at
/// this count, so a poller whose lease expired cannot overwrite a newer claim.</param>
/// <param name="Payload">The message bytes, passed to the broker unchanged.</param>
/// <param name="MessageId">An identifier for log messages only (for example the row's message GUID).</param>
public sealed record QueueMessage(long QueueId, int AttemptCount, byte[] Payload, object? MessageId = null);

/// <summary>What to write back for a handled <see cref="QueueMessage"/>.</summary>
/// <param name="Message">The claimed row.</param>
/// <param name="Status"><see cref="QueueMessageStatus.Completed"/>, <see cref="QueueMessageStatus.Pending"/> (retry later)
/// or <see cref="QueueMessageStatus.Failed"/> (terminal).</param>
/// <param name="Error">The failure text for <c>last_error</c>; null when completed.</param>
public readonly record struct QueueMessageOutcome(QueueMessage Message, QueueMessageStatus Status, string? Error)
{
    public static QueueMessageOutcome Completed(QueueMessage message) => new(message, QueueMessageStatus.Completed, null);
}
