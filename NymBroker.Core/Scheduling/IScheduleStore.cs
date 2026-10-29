namespace NymBroker.Core.Scheduling;

/// <summary>
/// Remembers the last run of each named scheduled action. The broker claims every occurrence here before it runs it,
/// so brokers that share a store run each occurrence once, and a <see cref="MissedRunPolicy.RunOnce"/> action can tell
/// after a restart that it missed a run. Without a registered store, named actions use an in-process store.
/// Register a database store with a <c>NymBroker.Scheduling.*</c> package, or your own with
/// <c>NymBrokerBuilder.AddScheduleStore</c>.
/// </summary>
/// <remarks>
/// Occurrences are passed in UTC and are whole milliseconds. Exceptions propagate: the broker logs them and skips the
/// occurrence (it never runs an occurrence it could not claim).
/// </remarks>
public interface IScheduleStore
{
    /// <summary>The latest occurrence claimed for <paramref name="name"/>, in UTC, or null if it never ran.</summary>
    ValueTask<DateTimeOffset?> GetLastRunAsync(string name, CancellationToken ct = default);

    /// <summary>
    /// Atomically records <paramref name="occurrence"/> as the last run of <paramref name="name"/> if no run is recorded
    /// or the recorded one is earlier. Returns true if this call recorded it, false if the occurrence (or a later one)
    /// was already claimed, e.g. by another instance.
    /// </summary>
    ValueTask<bool> TryClaimAsync(string name, DateTimeOffset occurrence, CancellationToken ct = default);
}
