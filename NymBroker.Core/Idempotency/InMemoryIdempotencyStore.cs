using System.Collections.Concurrent;

namespace NymBroker.Core.Idempotency;

public sealed class InMemoryIdempotencyStore : IIdempotencyStore
{
    private readonly ConcurrentDictionary<Guid, long> _seen = new(); // value = expiry ticks
    private readonly long _ttlTicks;
    private long _nextPruneTicks;

    public InMemoryIdempotencyStore(TimeSpan ttl)
    {
        _ttlTicks      = ttl.Ticks;
        _nextPruneTicks = (DateTime.UtcNow + ttl).Ticks;
    }

    public bool TryMarkSeen(Guid messageId)
    {
        var now    = DateTime.UtcNow.Ticks;
        var expiry = now + _ttlTicks;

        if (_seen.TryAdd(messageId, expiry))
        {
            MaybePrune(now);
            return true; // new — proceed
        }

        // Already present — check whether the stored entry has expired.
        if (_seen.TryGetValue(messageId, out var existing) && now > existing)
        {
            _seen[messageId] = expiry; // refresh
            return true;
        }

        return false; // genuine duplicate
    }

    private void MaybePrune(long nowTicks)
    {
        if (nowTicks < Volatile.Read(ref _nextPruneTicks)) return;
        Volatile.Write(ref _nextPruneTicks, nowTicks + _ttlTicks);
        foreach (var kvp in _seen)
            if (nowTicks > kvp.Value)
                _seen.TryRemove(kvp.Key, out _);
    }
}
