using NymBroker.Resilience;

namespace NymBroker.Tests;

public class RetryPolicyTests
{
    private static RetryPolicy Policy(int maxRetries = 3, Func<Exception, bool>? shouldHandle = null, List<RetryAttempt>? log = null)
        => new(new RetryOptions
        {
            MaxRetryAttempts = maxRetries,
            Delay            = TimeSpan.Zero,
            ShouldHandle     = shouldHandle ?? (static ex => ex is not OperationCanceledException),
            OnRetry          = a => { log?.Add(a); return ValueTask.CompletedTask; }
        });

    [Fact]
    public async Task ExecuteAsync_SucceedsFirstTry_RunsOnce_NoRetryCallback()
    {
        var log = new List<RetryAttempt>();
        var calls = 0;

        var result = await Policy(log: log).ExecuteAsync(_ => { calls++; return ValueTask.FromResult(42); }, TestContext.Current.CancellationToken);

        Assert.Equal(42, result);
        Assert.Equal(1, calls);
        Assert.Empty(log);
    }

    [Fact]
    public async Task ExecuteAsync_TransientFailures_ThenSuccess_ReturnsValue()
    {
        var log = new List<RetryAttempt>();
        var calls = 0;

        var result = await Policy(log: log).ExecuteAsync(_ =>
        {
            if (++calls < 3) throw new IOException($"fail {calls}");
            return ValueTask.FromResult("ok");
        }, TestContext.Current.CancellationToken);

        Assert.Equal("ok", result);
        Assert.Equal(3, calls);
        Assert.Equal([0, 1], log.Select(a => a.AttemptNumber));
        Assert.Equal("fail 1", log[0].Exception.Message);
    }

    [Fact]
    public async Task ExecuteAsync_ExhaustsRetries_RethrowsOriginalException()
    {
        var calls = 0;
        var thrown = new IOException("always");

        var ex = await Assert.ThrowsAsync<IOException>(async () =>
            await Policy(maxRetries: 4).ExecuteAsync(_ => { calls++; throw thrown; }, TestContext.Current.CancellationToken));

        Assert.Same(thrown, ex);
        Assert.Equal(5, calls);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldHandleFalse_DoesNotRetry()
    {
        var calls = 0;

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await Policy(shouldHandle: static ex => ex is IOException)
                .ExecuteAsync(_ => { calls++; throw new InvalidOperationException(); }, TestContext.Current.CancellationToken));

        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task ExecuteAsync_DefaultShouldHandle_DoesNotRetryOperationCanceled()
    {
        var calls = 0;
        var policy = new RetryPolicy(new RetryOptions { Delay = TimeSpan.Zero });

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await policy.ExecuteAsync(_ => { calls++; throw new OperationCanceledException(); }, TestContext.Current.CancellationToken));

        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task ExecuteAsync_CancelledDuringDelay_ThrowsAndStopsRetrying()
    {
        using var cts = new CancellationTokenSource();
        var calls = 0;
        var policy = new RetryPolicy(new RetryOptions
        {
            MaxRetryAttempts = 10,
            Delay            = TimeSpan.FromSeconds(30),
            OnRetry          = _ => { cts.Cancel(); return ValueTask.CompletedTask; }
        });

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await policy.ExecuteAsync(_ => { calls++; throw new IOException(); }, cts.Token));

        Assert.Equal(1, calls);
    }

    [Fact]
    public void Constructor_RejectsZeroRetries()
        => Assert.Throws<ArgumentOutOfRangeException>(() => new RetryPolicy(new RetryOptions { MaxRetryAttempts = 0 }));

    // ── delay math ───────────────────────────────────────────────────────────

    private static TimeSpan Delay(RetryBackoffType backoff, bool jitter, int attempt, TimeSpan? max = null, Func<double>? random = null)
    {
        var prev = 0d;
        return RetryPolicy.ComputeDelay(backoff, jitter, TimeSpan.FromMilliseconds(25), max ?? TimeSpan.FromDays(1),
            attempt, ref prev, random ?? (static () => 0.5));
    }

    [Fact]
    public void ComputeDelay_Constant_IsBaseDelay()
        => Assert.Equal(TimeSpan.FromMilliseconds(25), Delay(RetryBackoffType.Constant, jitter: false, attempt: 7));

    [Theory]
    [InlineData(0, 25)]
    [InlineData(1, 50)]
    [InlineData(2, 100)]
    [InlineData(3, 200)]
    public void ComputeDelay_Exponential_Doubles(int attempt, int expectedMs)
        => Assert.Equal(TimeSpan.FromMilliseconds(expectedMs), Delay(RetryBackoffType.Exponential, jitter: false, attempt));

    [Theory]
    [InlineData(0.0, 18.75)]
    [InlineData(1.0, 31.25)]
    public void ComputeDelay_ConstantJitter_IsWithinPlusMinus25Percent(double random, double expectedMs)
        => Assert.Equal(TimeSpan.FromMilliseconds(expectedMs), Delay(RetryBackoffType.Constant, jitter: true, 0, random: () => random));

    [Fact]
    public void ComputeDelay_ExponentialJitter_IsPositiveAndGrows()
    {
        var prev = 0d;
        var delays = Enumerable.Range(0, 6)
            .Select(i => RetryPolicy.ComputeDelay(RetryBackoffType.Exponential, true, TimeSpan.FromMilliseconds(25),
                TimeSpan.FromDays(1), i, ref prev, Random.Shared.NextDouble))
            .ToList();

        Assert.All(delays, d => Assert.True(d > TimeSpan.Zero));
        Assert.True(delays[^1] > delays[0]);
    }

    [Theory]
    [InlineData(RetryBackoffType.Exponential, false)]
    [InlineData(RetryBackoffType.Exponential, true)]
    public void ComputeDelay_HugeAttempt_ClampsToMaxDelay_WithoutOverflow(RetryBackoffType backoff, bool jitter)
        => Assert.Equal(TimeSpan.FromMinutes(5), Delay(backoff, jitter, int.MaxValue - 1, max: TimeSpan.FromMinutes(5)));
}
