using Microsoft.Extensions.Time.Testing;
using NymBroker.Core.Resilience;

namespace NymBroker.Tests;

/// <summary>State transitions of <see cref="CircuitBreaker"/> (#134), on a fake clock.</summary>
public sealed class CircuitBreakerTests
{
    private static readonly TimeSpan Break = TimeSpan.FromSeconds(30);

    private readonly FakeTimeProvider _time = new();

    private CircuitBreaker Create(int minimumThroughput = 4, double failureRatio = 0.5, Func<Exception, bool>? shouldHandle = null)
        => new(new CircuitBreakerOptions
        {
            MinimumThroughput = minimumThroughput,
            FailureRatio = failureRatio,
            SamplingDuration = TimeSpan.FromSeconds(10),
            BreakDuration = Break,
            ShouldHandle = shouldHandle ?? (static _ => true),
            TimeProvider = _time
        });

    private static async Task FailAsync(CircuitBreaker breaker, Exception? ex = null)
        => await Assert.ThrowsAnyAsync<Exception>(async () => await breaker.ExecuteAsync(_ => throw (ex ?? new IOException("down")), TestContext.Current.CancellationToken));

    private static ValueTask SucceedAsync(CircuitBreaker breaker) => breaker.ExecuteAsync(static _ => ValueTask.CompletedTask, TestContext.Current.CancellationToken);

    [Fact]
    public async Task StaysClosed_BelowMinimumThroughput()
    {
        var breaker = Create(minimumThroughput: 4);

        for (var i = 0; i < 3; i++) await FailAsync(breaker);

        Assert.Equal(CircuitState.Closed, breaker.State);
    }

    [Fact]
    public async Task Opens_AtFailureRatio_AndRejectsWithoutCalling()
    {
        var breaker = Create(minimumThroughput: 4, failureRatio: 0.5);
        await SucceedAsync(breaker);
        await SucceedAsync(breaker);
        await FailAsync(breaker);
        Assert.Equal(CircuitState.Closed, breaker.State);   // 1 of 3: below the throughput

        await FailAsync(breaker);                            // 2 of 4 = 0.5

        Assert.Equal(CircuitState.Open, breaker.State);
        var called = false;
        var ex = await Assert.ThrowsAsync<BrokenCircuitException>(async () => await breaker.ExecuteAsync(_ => { called = true; return ValueTask.CompletedTask; }, TestContext.Current.CancellationToken));
        Assert.False(called);
        Assert.Equal(Break, ex.RetryAfter);
    }

    [Fact]
    public async Task StaysClosed_WhileFailureRatioIsBelowThreshold()
    {
        var breaker = Create(minimumThroughput: 4, failureRatio: 0.5);
        for (var i = 0; i < 3; i++) await SucceedAsync(breaker);

        await FailAsync(breaker);   // 1 of 4

        Assert.Equal(CircuitState.Closed, breaker.State);
    }

    [Fact]
    public async Task OldOutcomes_LeaveTheRollingWindow()
    {
        var breaker = Create(minimumThroughput: 4, failureRatio: 1);
        for (var i = 0; i < 3; i++) await FailAsync(breaker);

        _time.Advance(TimeSpan.FromSeconds(11));   // the sampling window is 10 s
        await FailAsync(breaker);

        Assert.Equal(CircuitState.Closed, breaker.State);
    }

    [Fact]
    public async Task AfterTheBreak_ReportsHalfOpen_AndASuccessfulTrialCloses()
    {
        var breaker = Create(minimumThroughput: 1, failureRatio: 1);
        await FailAsync(breaker);
        Assert.Equal(CircuitState.Open, breaker.State);

        _time.Advance(Break);
        Assert.Equal(CircuitState.HalfOpen, breaker.State);

        await SucceedAsync(breaker);
        Assert.Equal(CircuitState.Closed, breaker.State);
    }

    [Fact]
    public async Task AFailedTrial_OpensAgainForAFullBreak()
    {
        var breaker = Create(minimumThroughput: 1, failureRatio: 1);
        await FailAsync(breaker);
        _time.Advance(Break);

        await FailAsync(breaker);

        Assert.Equal(CircuitState.Open, breaker.State);
        _time.Advance(Break - TimeSpan.FromSeconds(1));
        Assert.Equal(CircuitState.Open, breaker.State);
    }

    [Fact]
    public async Task ClosingResetsTheWindow()
    {
        var breaker = Create(minimumThroughput: 2, failureRatio: 1);
        await FailAsync(breaker);
        await FailAsync(breaker);
        _time.Advance(Break);
        await SucceedAsync(breaker);

        await FailAsync(breaker);   // the failures before the break no longer count

        Assert.Equal(CircuitState.Closed, breaker.State);
    }

    [Fact]
    public async Task OnlyOneTrialAtATime()
    {
        var breaker = Create(minimumThroughput: 1, failureRatio: 1);
        await FailAsync(breaker);
        _time.Advance(Break);

        var release = new TaskCompletionSource();
        var trial = breaker.ExecuteAsync(async _ => await release.Task, TestContext.Current.CancellationToken).AsTask();

        var ex = await Assert.ThrowsAsync<BrokenCircuitException>(async () => await SucceedAsync(breaker));
        Assert.Null(ex.RetryAfter);

        release.SetResult();
        await trial;
        Assert.Equal(CircuitState.Closed, breaker.State);
    }

    [Fact]
    public async Task CallerCancellation_IsNotCounted()
    {
        var breaker = Create(minimumThroughput: 1, failureRatio: 1);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await breaker.ExecuteAsync(token => { token.ThrowIfCancellationRequested(); return ValueTask.CompletedTask; }, cts.Token));

        Assert.Equal(CircuitState.Closed, breaker.State);
    }

    [Fact]
    public async Task ACancellationTheCallRaisedItself_IsAFailure()
    {
        var breaker = Create(minimumThroughput: 1, failureRatio: 1);

        await FailAsync(breaker, new TaskCanceledException("HttpClient.Timeout"));

        Assert.Equal(CircuitState.Open, breaker.State);
    }

    [Fact]
    public async Task ExceptionsShouldHandleRejects_AreNotCounted()
    {
        var breaker = Create(minimumThroughput: 1, failureRatio: 1, shouldHandle: static ex => ex is IOException);

        await FailAsync(breaker, new ArgumentException("bad input"));

        Assert.Equal(CircuitState.Closed, breaker.State);
    }

    [Fact]
    public async Task ExecuteAsyncWithResult_ReturnsTheResult()
    {
        var breaker = Create();

        Assert.Equal(42, await breaker.ExecuteAsync(static _ => ValueTask.FromResult(42), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task WaitForPermit_HoldsUntilTheBreakIsOver()
    {
        var breaker = Create(minimumThroughput: 1, failureRatio: 1);
        await FailAsync(breaker);

        var wait = breaker.WaitForPermitAsync(TestContext.Current.CancellationToken).AsTask();
        Assert.False(wait.IsCompleted);

        _time.Advance(Break);
        var (permit, waited) = await wait.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.True(waited);
        Assert.True(permit.IsTrial);
        breaker.RecordSuccess(permit);
        Assert.Equal(CircuitState.Closed, breaker.State);
    }

    [Fact]
    public async Task WaitForPermit_WaitsForTheRunningTrial()
    {
        var breaker = Create(minimumThroughput: 1, failureRatio: 1);
        await FailAsync(breaker);
        _time.Advance(Break);
        var (trial, _) = await breaker.WaitForPermitAsync(TestContext.Current.CancellationToken);

        var second = breaker.WaitForPermitAsync(TestContext.Current.CancellationToken).AsTask();
        Assert.False(second.IsCompleted);

        breaker.RecordSuccess(trial);
        var (permit, waited) = await second.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.True(waited);
        Assert.False(permit.IsTrial);
    }

    [Fact]
    public async Task WaitForPermit_EndsWhenCancelled()
    {
        var breaker = Create(minimumThroughput: 1, failureRatio: 1);
        await FailAsync(breaker);
        using var cts = new CancellationTokenSource();

        var wait = breaker.WaitForPermitAsync(cts.Token).AsTask();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait);
    }

    [Fact]
    public async Task AnIgnoredTrial_LetsTheNextCallTry()
    {
        var breaker = Create(minimumThroughput: 1, failureRatio: 1);
        await FailAsync(breaker);
        _time.Advance(Break);
        var (trial, _) = await breaker.WaitForPermitAsync(TestContext.Current.CancellationToken);

        breaker.RecordIgnored(trial);

        Assert.Equal(CircuitState.HalfOpen, breaker.State);
        var (next, waited) = await breaker.WaitForPermitAsync(TestContext.Current.CancellationToken);
        Assert.False(waited);
        Assert.True(next.IsTrial);
    }

    private static readonly CircuitBreakerOptions[] InvalidOptions =
    [
        new() { FailureRatio = 0 },
        new() { FailureRatio = 1.1 },
        new() { FailureRatio = double.NaN },
        new() { MinimumThroughput = 0 },
        new() { SamplingDuration = TimeSpan.FromMilliseconds(5) },
        new() { BreakDuration = TimeSpan.Zero },
        new() { BreakDuration = TimeSpan.FromDays(2) },
        new() { ShouldHandle = null! },
        new() { TimeProvider = null! }
    ];

    [Theory]
    [InlineData(0)] [InlineData(1)] [InlineData(2)] [InlineData(3)] [InlineData(4)]
    [InlineData(5)] [InlineData(6)] [InlineData(7)] [InlineData(8)]
    public void InvalidOptions_Throw(int index)
        => Assert.ThrowsAny<ArgumentException>(() => new CircuitBreaker(InvalidOptions[index]));
}
