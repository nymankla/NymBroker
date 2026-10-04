using System.Diagnostics.Metrics;

namespace NymBroker.Resilience;

/// <summary>
/// Immutable, thread-safe retry policy. Build once and reuse across calls.
/// </summary>
public sealed class RetryPolicy
{
    public const string InstrumentationName = "NymBroker.Resilience";

    private static readonly Meter Meter = new(InstrumentationName);
    private static readonly Counter<long> RetryAttempts =
        Meter.CreateCounter<long>("nymbroker.retries", "{retry}");

    private static readonly TimeSpan DefaultMaxDelay = TimeSpan.FromDays(1);

    private readonly RetryOptions _options;
    private readonly TimeSpan _maxDelay;

    public RetryPolicy(RetryOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.MaxRetryAttempts, 1, nameof(options.MaxRetryAttempts));
        ArgumentOutOfRangeException.ThrowIfLessThan(options.Delay, TimeSpan.Zero, nameof(options.Delay));
        ArgumentNullException.ThrowIfNull(options.ShouldHandle, nameof(options.ShouldHandle));
        ArgumentNullException.ThrowIfNull(options.TimeProvider, nameof(options.TimeProvider));

        _options  = options;
        _maxDelay = options.MaxDelay ?? DefaultMaxDelay;
    }

    public async ValueTask ExecuteAsync(Func<CancellationToken, ValueTask> action, CancellationToken ct = default)
    {
        var attempt = 0;
        var prev    = 0d;
        while (true)
        {
            try
            {
                await action(ct).ConfigureAwait(false);
                return;
            }
            // The filter leaves the final / unhandled exception propagating with its original stack.
            catch (Exception ex) when (CanRetry(ex, attempt, ct))
            {
                await WaitBeforeRetryAsync(ex, attempt, ref prev, ct).ConfigureAwait(false);
                attempt++;
            }
        }
    }

    public async ValueTask<T> ExecuteAsync<T>(Func<CancellationToken, ValueTask<T>> action, CancellationToken ct = default)
    {
        var attempt = 0;
        var prev    = 0d;
        while (true)
        {
            try
            {
                return await action(ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (CanRetry(ex, attempt, ct))
            {
                await WaitBeforeRetryAsync(ex, attempt, ref prev, ct).ConfigureAwait(false);
                attempt++;
            }
        }
    }

    private bool CanRetry(Exception ex, int attempt, CancellationToken ct)
        => attempt < _options.MaxRetryAttempts && !ct.IsCancellationRequested && _options.ShouldHandle(ex);

    // Not async (ref param); computes the delay synchronously and returns the awaitable.
    private ValueTask WaitBeforeRetryAsync(Exception ex, int attempt, ref double prev, CancellationToken ct)
    {
        var delay = ComputeDelay(_options.BackoffType, _options.UseJitter, _options.Delay, _maxDelay, attempt, ref prev, Random.Shared.NextDouble);
        return OnRetryThenDelayAsync(new RetryAttempt(attempt, ex, delay), ct);
    }

    private async ValueTask OnRetryThenDelayAsync(RetryAttempt args, CancellationToken ct)
    {
        RetryAttempts.Add(1);
        if (_options.OnRetry != null)
            await _options.OnRetry(args).ConfigureAwait(false);
        if (args.Delay > TimeSpan.Zero)
            await Task.Delay(args.Delay, _options.TimeProvider, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Delay before the retry following 0-based <paramref name="attempt"/>. <paramref name="prev"/> carries
    /// decorrelated-jitter state between attempts (start at 0). Result is always within [0, maxDelay].
    /// </summary>
    internal static TimeSpan ComputeDelay(
        RetryBackoffType backoff, bool useJitter, TimeSpan baseDelay, TimeSpan maxDelay,
        int attempt, ref double prev, Func<double> random)
    {
        if (baseDelay == TimeSpan.Zero) return TimeSpan.Zero;

        double ticks;
        if (backoff == RetryBackoffType.Constant)
        {
            ticks = baseDelay.Ticks;
            if (useJitter)
                ticks += (random() * 0.5 - 0.25) * ticks; // ±25%
        }
        else if (useJitter)
        {
            ticks = DecorrelatedJitterTicks(attempt, baseDelay, ref prev, random);
        }
        else
        {
            // Computed in double so huge attempt counts saturate to +Infinity instead of overflowing.
            ticks = baseDelay.Ticks * Math.Pow(2, attempt);
        }

        if (double.IsNaN(ticks) || ticks <= 0) return TimeSpan.Zero;
        return ticks >= maxDelay.Ticks ? maxDelay : TimeSpan.FromTicks((long)ticks);
    }

    // "Decorrelated jitter backoff V2" (Polly.Contrib.WaitAndRetry, also used by Polly v8): exponential
    // growth with a smooth random spread, so retries from many callers don't synchronize.
    private static double DecorrelatedJitterTicks(int attempt, TimeSpan baseDelay, ref double prev, Func<double> random)
    {
        const double pFactor         = 4.0;
        const double rpScalingFactor = 1 / 1.4d;

        var t    = attempt + random();
        var next = Math.Pow(2, t) * Math.Tanh(Math.Sqrt(pFactor * t));
        if (double.IsInfinity(next))
        {
            prev = next;
            return double.PositiveInfinity;
        }

        var intrinsic = next - prev;
        prev = next;
        return intrinsic * rpScalingFactor * baseDelay.Ticks;
    }
}
