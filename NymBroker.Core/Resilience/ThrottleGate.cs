using System.Diagnostics;

namespace NymBroker.Core.Resilience;

/// <summary>
/// A consumer's or an endpoint's concurrency and rate limit (#136). <see cref="EnterAsync"/> waits for a concurrency
/// slot, then for rate permits, and returns a lease that frees the slot. Waiting is the point: it is backpressure,
/// never a failure. Only the caller's token ends a wait.
/// </summary>
internal sealed class ThrottleGate
{
    private readonly SemaphoreSlim? _concurrency;
    private readonly TokenBucket? _rate;
    private readonly TagList _tags;

    public ThrottleGate(string scope, string name, RateLimitOptions? rateLimit, int? concurrencyLimit)
    {
        if (rateLimit is not null) _rate = new TokenBucket(rateLimit);
        if (concurrencyLimit is { } limit) _concurrency = new SemaphoreSlim(limit, limit);
        RateLimit = rateLimit;
        ConcurrencyLimit = concurrencyLimit;
        _tags = new TagList { { "scope", scope }, { "name", name } };
    }

    public RateLimitOptions? RateLimit { get; }

    public int? ConcurrencyLimit { get; }

    /// <summary>A gate for these limits, or null when there are none.</summary>
    public static ThrottleGate? For(string scope, string name, RateLimitOptions? rateLimit, int? concurrencyLimit)
        => rateLimit is null && concurrencyLimit is null ? null : new ThrottleGate(scope, name, rateLimit, concurrencyLimit);

    /// <summary>Waits until a call with <paramref name="permits"/> rate permits may start; dispose the lease when it is done.</summary>
    public async ValueTask<Lease> EnterAsync(int permits, CancellationToken ct)
    {
        var started = Stopwatch.GetTimestamp();
        var waited = false;
        if (_concurrency is not null && !_concurrency.Wait(0, CancellationToken.None))
        {
            Record("concurrency");
            waited = true;
            await _concurrency.WaitAsync(ct).ConfigureAwait(false);
        }

        try
        {
            if (_rate is not null && await _rate.AcquireAsync(permits, ct).ConfigureAwait(false) > TimeSpan.Zero)
            {
                Record("rate");
                waited = true;
            }
        }
        catch
        {
            _concurrency?.Release();
            throw;
        }

        if (waited) ResilienceDiagnostics.ThrottleWaitDuration.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds, _tags);
        return new Lease(_concurrency);
    }

    private void Record(string limit)
    {
        var tags = _tags;
        tags.Add("limit", limit);
        ResilienceDiagnostics.ThrottleWaits.Add(1, tags);
    }

    /// <summary>Frees the concurrency slot taken by <see cref="EnterAsync"/>.</summary>
    internal readonly struct Lease(SemaphoreSlim? concurrency) : IDisposable
    {
        public void Dispose() => concurrency?.Release();
    }
}
