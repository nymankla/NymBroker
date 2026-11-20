namespace NymBroker.Core.Endpoint.Queue;

/// <summary>
/// Moves selected failed messages of a queue endpoint back to pending, so its listener handles them again. Obtained through
/// <see cref="INymBroker.GetQueueReplayer"/>, which also logs every replay and counts it; a transport that cannot offer it
/// gives none. Kept apart from the read-only <see cref="IQueueInspector"/> so an application can allow viewing a queue
/// without allowing replay.
/// </summary>
/// <remarks>
/// A replayed message keeps its envelope (same id, same payload) and its place in the queue, so it is handled before newer
/// pending messages. Its attempt count starts again from zero. Replaying runs the consumer's side effects again: consumers
/// must be idempotent.
/// </remarks>
public interface IQueueReplayer
{
    /// <summary>
    /// Replays each of <paramref name="messages"/> that is still failed with the same <see cref="QueueMessageInfo.FailedAt"/>;
    /// the others are skipped (replayed already, failed again since, or removed by retention). One call is atomic: on an
    /// error nothing is replayed, and calling again with the same selection is safe.
    /// </summary>
    /// <param name="messages">Failed messages as listed by <see cref="IQueueInspector.GetMessagesAsync"/>, 1 to <see cref="IQueueInspector.MaxPageSize"/>.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="messages"/> is empty or larger than <see cref="IQueueInspector.MaxPageSize"/>.</exception>
    /// <exception cref="ArgumentException">A message is not <see cref="QueueInspectionState.Failed"/>, has no <see cref="QueueMessageInfo.FailedAt"/>, or its id is not one of this endpoint's.</exception>
    Task<QueueReplayResult> ReplayFailedAsync(IReadOnlyCollection<QueueMessageInfo> messages, CancellationToken ct = default);
}

/// <summary>The outcome of <see cref="IQueueReplayer.ReplayFailedAsync"/>.</summary>
/// <param name="Replayed">Ids of the messages now pending again.</param>
/// <param name="Skipped">Ids that were no longer failed with the given failure time: replayed already, failed again since, or removed by retention.</param>
public sealed record QueueReplayResult(IReadOnlyList<string> Replayed, IReadOnlyList<string> Skipped);
