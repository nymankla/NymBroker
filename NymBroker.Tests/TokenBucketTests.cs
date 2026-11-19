using Microsoft.Extensions.Time.Testing;
using NymBroker.Core.Resilience;

namespace NymBroker.Tests;

/// <summary>The rate limiter behind <c>RateLimit</c> and <c>LimitSendsTo</c> (#136), and the gate that adds the concurrency limit.</summary>
public sealed class TokenBucketTests
{
    private readonly FakeTimeProvider _time = new();

    private TokenBucket Create(int permits = 1, double periodSeconds = 1, int? burst = null)
        => new(new RateLimitOptions { PermitsPerPeriod = permits, Period = TimeSpan.FromSeconds(periodSeconds), Burst = burst, TimeProvider = _time });

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task TheBurst_IsGrantedAtOnce_ThenCallersWaitForTheRate()
    {
        var bucket = Create(permits: 10, burst: 3);   // one permit every 100 ms

        for (var i = 0; i < 3; i++)
            Assert.Equal(TimeSpan.Zero, await bucket.AcquireAsync(1, Ct));

        var fourth = bucket.AcquireAsync(1, Ct).AsTask();
        Assert.False(fourth.IsCompleted);
        _time.Advance(TimeSpan.FromMilliseconds(100));
        Assert.Equal(TimeSpan.FromMilliseconds(100), await fourth.WaitAsync(TimeSpan.FromSeconds(5), Ct));
    }

    [Fact]
    public async Task Waiters_AreServedInArrivalOrder()
    {
        var bucket = Create(permits: 1);
        await bucket.AcquireAsync(1, Ct);

        var first = bucket.AcquireAsync(1, Ct).AsTask();
        var second = bucket.AcquireAsync(1, Ct).AsTask();

        _time.Advance(TimeSpan.FromSeconds(1));
        await first.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        Assert.False(second.IsCompleted);
        _time.Advance(TimeSpan.FromSeconds(1));
        await second.WaitAsync(TimeSpan.FromSeconds(5), Ct);
    }

    [Fact]
    public async Task ABatch_TakesOnePermitPerMessage()
    {
        var bucket = Create(permits: 1);

        var batch = bucket.AcquireAsync(3, Ct).AsTask();   // permits at 0, 1 and 2 s

        _time.Advance(TimeSpan.FromSeconds(1));
        Assert.False(batch.IsCompleted);
        _time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(TimeSpan.FromSeconds(2), await batch.WaitAsync(TimeSpan.FromSeconds(5), Ct));
    }

    [Fact]
    public async Task AfterAQuietTime_TheBurstIsBack()
    {
        var bucket = Create(permits: 2, burst: 2);
        await bucket.AcquireAsync(2, Ct);

        _time.Advance(TimeSpan.FromSeconds(5));

        Assert.Equal(TimeSpan.Zero, await bucket.AcquireAsync(1, Ct));
        Assert.Equal(TimeSpan.Zero, await bucket.AcquireAsync(1, Ct));
    }

    [Fact]
    public async Task ACancelledLatestWaiter_GivesItsSlotBack()
    {
        var bucket = Create(permits: 1);
        await bucket.AcquireAsync(1, Ct);
        using var cts = new CancellationTokenSource();

        var cancelled = bucket.AcquireAsync(1, cts.Token).AsTask();
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cancelled);

        var next = bucket.AcquireAsync(1, Ct).AsTask();
        _time.Advance(TimeSpan.FromSeconds(1));   // not 2 s: the cancelled reservation was given back
        await next.WaitAsync(TimeSpan.FromSeconds(5), Ct);
    }

    [Fact]
    public void ToString_DescribesTheLimit()
    {
        Assert.Equal("50/1s, burst 50", new RateLimitOptions { PermitsPerPeriod = 50 }.ToString());
        Assert.Equal("5/250ms, burst 1", new RateLimitOptions { PermitsPerPeriod = 5, Period = TimeSpan.FromMilliseconds(250), Burst = 1 }.ToString());
    }

    private static readonly RateLimitOptions[] InvalidOptions =
    [
        new() { PermitsPerPeriod = 0 },
        new() { Period = TimeSpan.Zero },
        new() { Period = TimeSpan.FromDays(2) },
        new() { Burst = 0 },
        new() { PermitsPerPeriod = 100, Period = TimeSpan.FromTicks(10) },
        new() { TimeProvider = null! }
    ];

    [Theory]
    [InlineData(0)] [InlineData(1)] [InlineData(2)] [InlineData(3)] [InlineData(4)] [InlineData(5)]
    public void InvalidOptions_Throw(int index)
        => Assert.ThrowsAny<ArgumentException>(() => new TokenBucket(InvalidOptions[index]));

    // --- ThrottleGate: concurrency, then rate ---

    [Fact]
    public async Task Gate_ConcurrencyLimit_WaitsForAFreeSlot()
    {
        var gate = new ThrottleGate("consumer", "test", rateLimit: null, concurrencyLimit: 1);
        var first = await gate.EnterAsync(1, Ct);

        var second = gate.EnterAsync(1, Ct).AsTask();
        Assert.False(second.IsCompleted);

        first.Dispose();
        (await second.WaitAsync(TimeSpan.FromSeconds(5), Ct)).Dispose();
    }

    [Fact]
    public async Task Gate_CancelledWhileWaitingForTheRate_FreesItsSlot()
    {
        var gate = new ThrottleGate("consumer", "test",
            new RateLimitOptions { PermitsPerPeriod = 1, TimeProvider = _time }, concurrencyLimit: 1);
        (await gate.EnterAsync(1, Ct)).Dispose();   // uses the only permit of this second
        using var cts = new CancellationTokenSource();

        var waiting = gate.EnterAsync(1, cts.Token).AsTask();
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);

        var next = gate.EnterAsync(1, Ct).AsTask();   // the slot is free again; only the rate makes it wait
        _time.Advance(TimeSpan.FromSeconds(1));
        (await next.WaitAsync(TimeSpan.FromSeconds(5), Ct)).Dispose();
    }

    [Fact]
    public void Gate_For_WithoutLimits_IsNull()
        => Assert.Null(ThrottleGate.For("endpoint", "x", null, null));
}
