namespace NymBroker.Core.Resilience;

/// <summary>
/// A rate limiter as a reservation queue (the generic cell rate algorithm): each caller reserves the next free slot
/// at once and waits until it is due, so waiters are served in arrival order and the rate is never exceeded, with no
/// timer or waiter list. Thread-safe.
/// </summary>
internal sealed class TokenBucket
{
    private static readonly TimeSpan MaxDelayStep = TimeSpan.FromDays(1);   // Task.Delay rejects ~49 days and more

    private readonly TimeProvider _time;
    private readonly long _intervalTicks;    // one permit every interval
    private readonly long _toleranceTicks;   // (burst - 1) intervals may be taken early
    private readonly Lock _lock = new();
    private long _theoreticalArrival;        // UTC ticks: when the next permit is due if no burst is left (guarded by _lock)

    public TokenBucket(RateLimitOptions options)
    {
        options.Validate();
        _time = options.TimeProvider;
        _intervalTicks = options.Period.Ticks / options.PermitsPerPeriod;
        _toleranceTicks = _intervalTicks * (options.EffectiveBurst - 1);
    }

    /// <summary>
    /// Waits until <paramref name="permits"/> permits are free; returns how long it waited. Cancelling gives the
    /// reservation back when it is still the latest one; otherwise the slot stays unused, so the limit holds either way.
    /// </summary>
    public async ValueTask<TimeSpan> AcquireAsync(int permits, CancellationToken ct)
    {
        var (wait, reservedUntil) = Reserve(permits);
        if (wait <= TimeSpan.Zero) return TimeSpan.Zero;

        try
        {
            for (var left = wait; left > TimeSpan.Zero; left -= MaxDelayStep)
                await Task.Delay(left < MaxDelayStep ? left : MaxDelayStep, _time, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            GiveBack(permits, reservedUntil);
            throw;
        }
        return wait;
    }

    private (TimeSpan Wait, long ReservedUntil) Reserve(int permits)
    {
        var now = _time.GetUtcNow().UtcTicks;
        lock (_lock)
        {
            var start = Math.Max(_theoreticalArrival, now);
            var lastDue = start + (permits - 1) * _intervalTicks;   // when the last of the permits may go
            _theoreticalArrival = lastDue + _intervalTicks;
            return (TimeSpan.FromTicks(Math.Max(0, lastDue - _toleranceTicks - now)), _theoreticalArrival);
        }
    }

    private void GiveBack(int permits, long reservedUntil)
    {
        lock (_lock)
            if (_theoreticalArrival == reservedUntil)
                _theoreticalArrival -= permits * _intervalTicks;
    }
}
