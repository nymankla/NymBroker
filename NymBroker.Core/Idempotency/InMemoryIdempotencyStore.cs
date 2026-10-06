using System.Collections.Concurrent;

namespace NymBroker.Core.Idempotency;

/// <summary>In-process <see cref="IIdempotencyStore"/>: duplicates are detected per process and forgotten on restart.</summary>
public sealed class InMemoryIdempotencyStore : IIdempotencyStore
{
    public static readonly TimeSpan DefaultLeaseTimeout = TimeSpan.FromMinutes(5);

    private readonly record struct Entry(bool Completed, long ExpiresTicks);

    private readonly ConcurrentDictionary<Guid, Entry> _entries = new();
    private readonly long _ttlTicks;
    private readonly long _leaseTicks;
    private readonly long _pruneIntervalTicks;
    private long _nextPruneTicks;

    /// <param name="ttl">How long a processed message ID is remembered.</param>
    /// <param name="leaseTimeout">How long a claim lasts if it is never completed or released (default 5 minutes).</param>
    public InMemoryIdempotencyStore(TimeSpan ttl, TimeSpan? leaseTimeout = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(ttl, TimeSpan.Zero);
        var lease = leaseTimeout ?? DefaultLeaseTimeout;
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(lease, TimeSpan.Zero, nameof(leaseTimeout));

        _ttlTicks = ttl.Ticks;
        _leaseTicks = lease.Ticks;
        _pruneIntervalTicks = Math.Max(_ttlTicks, _leaseTicks);
        _nextPruneTicks = DateTime.UtcNow.Ticks + _pruneIntervalTicks;
    }

    public ValueTask<IdempotencyClaimResult> TryClaimAsync(Guid messageId, CancellationToken ct = default)
    {
        var now = DateTime.UtcNow.Ticks;
        var claim = new Entry(false, now + _leaseTicks);

        while (true)
        {
            if (_entries.TryAdd(messageId, claim))
            {
                MaybePrune(now);
                return ValueTask.FromResult(IdempotencyClaimResult.Claimed);
            }

            if (!_entries.TryGetValue(messageId, out var existing))
                continue;   // removed in between — try to add again

            if (existing.ExpiresTicks > now)
                return ValueTask.FromResult(existing.Completed ? IdempotencyClaimResult.Duplicate : IdempotencyClaimResult.InProgress);

            // Expired record or lease: take it over, unless another caller just did.
            if (_entries.TryUpdate(messageId, claim, existing))
                return ValueTask.FromResult(IdempotencyClaimResult.Claimed);
        }
    }

    public ValueTask CompleteAsync(Guid messageId, CancellationToken ct = default)
    {
        _entries[messageId] = new Entry(true, DateTime.UtcNow.Ticks + _ttlTicks);
        return ValueTask.CompletedTask;
    }

    public ValueTask ReleaseAsync(Guid messageId, CancellationToken ct = default)
    {
        // Only an open claim is released; a completed record stays.
        if (_entries.TryGetValue(messageId, out var existing) && !existing.Completed)
            _entries.TryRemove(new KeyValuePair<Guid, Entry>(messageId, existing));
        return ValueTask.CompletedTask;
    }

    private void MaybePrune(long nowTicks)
    {
        var next = Volatile.Read(ref _nextPruneTicks);
        if (nowTicks < next || Interlocked.CompareExchange(ref _nextPruneTicks, nowTicks + _pruneIntervalTicks, next) != next)
            return;

        foreach (var kvp in _entries)
            if (nowTicks > kvp.Value.ExpiresTicks)
                _entries.TryRemove(kvp);
    }
}
