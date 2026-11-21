namespace NymBroker.Core.Endpoint.Queue;

/// <summary>
/// Deletes selected failed messages of a queue endpoint, for an operator who has reviewed a failure and decided it needs
/// nothing more. Obtained through <see cref="INymBroker.GetQueueFailedRemover"/>, which also logs every removal and counts
/// it; a transport that cannot offer it gives none. Separate from <see cref="IQueueReplayer"/> and the read-only
/// <see cref="IQueueInspector"/>, so an application can allow each operation on its own.
/// </summary>
/// <remarks>
/// Only failed messages are removed; pending, in-progress and deferred messages are never touched. A removed message is
/// gone: it cannot be replayed afterwards.
/// </remarks>
public interface IQueueFailedRemover
{
    /// <summary>
    /// Removes each of <paramref name="messages"/> that is still failed with the same <see cref="QueueMessageInfo.FailedAt"/>;
    /// the others are skipped (replayed, failed again since, or removed already, for example by retention). One call is
    /// atomic: on an error nothing is removed, and calling again with the same selection is safe.
    /// </summary>
    /// <param name="messages">Failed messages as listed by <see cref="IQueueInspector.GetMessagesAsync"/>, 1 to <see cref="IQueueInspector.MaxPageSize"/>.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="messages"/> is empty or larger than <see cref="IQueueInspector.MaxPageSize"/>.</exception>
    /// <exception cref="ArgumentException">A message is not <see cref="QueueInspectionState.Failed"/>, has no <see cref="QueueMessageInfo.FailedAt"/>, or its id is not one of this endpoint's.</exception>
    Task<QueueRemoveResult> RemoveFailedAsync(IReadOnlyCollection<QueueMessageInfo> messages, CancellationToken ct = default);
}

/// <summary>The outcome of <see cref="IQueueFailedRemover.RemoveFailedAsync"/>.</summary>
/// <param name="Removed">Ids of the messages deleted.</param>
/// <param name="Skipped">Ids that were no longer failed with the given failure time: replayed, failed again since, or removed already.</param>
public sealed record QueueRemoveResult(IReadOnlyList<string> Removed, IReadOnlyList<string> Skipped);
