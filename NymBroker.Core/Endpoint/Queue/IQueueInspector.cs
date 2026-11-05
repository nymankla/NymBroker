namespace NymBroker.Core.Endpoint.Queue;

/// <summary>
/// Read-only view of what is waiting on, or has failed in, a queue endpoint, so an application can show queue status
/// without knowing the transport's storage. Obtained through <see cref="INymBroker.GetQueueInspector"/>; a transport that
/// cannot offer it gives none.
/// </summary>
public interface IQueueInspector
{
    /// <summary>Number of messages in each <see cref="QueueInspectionState"/>, read as one consistent snapshot.</summary>
    Task<QueueCounts> GetCountsAsync(CancellationToken ct = default);

    /// <summary>
    /// One page of messages in <paramref name="state"/>, oldest first (deferred messages by delivery time). Pass the previous
    /// page's <see cref="QueuePage.NextCursor"/> as <paramref name="cursor"/> to continue; a cursor is only valid for the same
    /// <paramref name="state"/>.
    /// </summary>
    /// <param name="state">Which messages to list. <see cref="QueueInspectionState.Completed"/> is not listable.</param>
    /// <param name="limit">Page size, 1 to <see cref="MaxPageSize"/>.</param>
    /// <param name="cursor">null for the first page.</param>
    /// <param name="failedSince">With <see cref="QueueInspectionState.Failed"/>: only messages that failed at or after this time.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="limit"/> is out of range, or the state is not listable.</exception>
    /// <exception cref="ArgumentException"><paramref name="cursor"/> is not a cursor from this inspector.</exception>
    Task<QueuePage> GetMessagesAsync(QueueInspectionState state, int limit = 100, string? cursor = null,
        DateTimeOffset? failedSince = null, CancellationToken ct = default);

    /// <summary>The largest allowed page size.</summary>
    const int MaxPageSize = 1000;
}

/// <summary>What a queue message is waiting for.</summary>
public enum QueueInspectionState
{
    /// <summary>Due and waiting to be claimed (including a message returned for retry).</summary>
    Pending,
    /// <summary>Claimed by a listener and being handled (or its lease has not yet expired).</summary>
    InProgress,
    /// <summary>Posted for a later time and not yet due.</summary>
    Deferred,
    /// <summary>Gave up after its retries, or was dead-lettered by the transport.</summary>
    Failed,
    /// <summary>Handled and kept until retention removes it. Counted only.</summary>
    Completed
}

/// <summary>Message counts per state.</summary>
public sealed record QueueCounts(long Pending, long InProgress, long Deferred, long Failed, long Completed)
{
    /// <summary>Messages that have not finished: pending, in progress and deferred.</summary>
    public long Active => Pending + InProgress + Deferred;
}

/// <summary>A page of messages and the cursor to the next one (null when this was the last).</summary>
public sealed record QueuePage(IReadOnlyList<QueueMessageInfo> Messages, string? NextCursor);

/// <summary>One message as seen by an inspector.</summary>
/// <param name="Id">The message's identifier, the same in every state (a deferred message keeps it when it becomes due).</param>
/// <param name="State">The message's state.</param>
/// <param name="Attempts">Times it has been claimed (0 for a deferred message).</param>
/// <param name="CreatedAt">When it was posted, or became due for a deferred message.</param>
/// <param name="DeliverAt">For a deferred message, when it becomes due; otherwise null.</param>
/// <param name="FailedAt">For a failed message, when it failed; otherwise null.</param>
/// <param name="LastError">The last failure text, if any.</param>
/// <param name="Payload">The message bytes as posted (the broker's JSON envelope).</param>
public sealed record QueueMessageInfo(string Id, QueueInspectionState State, int Attempts, DateTimeOffset CreatedAt,
    DateTimeOffset? DeliverAt, DateTimeOffset? FailedAt, string? LastError, ReadOnlyMemory<byte> Payload);
