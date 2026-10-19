using NymBroker.Core.Endpoint;

namespace NymBroker.Tests;

public sealed class AsyncOnceTests
{
    [Fact]
    public async Task RunAsync_ConcurrentCallers_RunTheActionOnce()
    {
        var once = new AsyncOnce();
        var runs = 0;
        var gate = new TaskCompletionSource();

        var callers = Enumerable.Range(0, 10).Select(_ => once.RunAsync(gate, async (g, _) =>
        {
            Interlocked.Increment(ref runs);
            await g.Task;
        }, TestContext.Current.CancellationToken)).ToList();
        gate.SetResult();
        await Task.WhenAll(callers);

        Assert.Equal(1, runs);
        Assert.True(once.IsCompleted);
    }

    [Fact]
    public async Task RunAsync_FailedAttempt_IsRetriedByTheNextCaller()
    {
        var once = new AsyncOnce();
        var runs = 0;

        await Assert.ThrowsAsync<InvalidOperationException>(() => once.RunAsync(0, (_, _) =>
        {
            runs++;
            throw new InvalidOperationException("database down");
        }, TestContext.Current.CancellationToken));
        Assert.False(once.IsCompleted);

        await once.RunAsync(0, (_, _) => { runs++; return Task.CompletedTask; }, TestContext.Current.CancellationToken);
        await once.RunAsync(0, (_, _) => { runs++; return Task.CompletedTask; }, TestContext.Current.CancellationToken);

        Assert.Equal(2, runs);
        Assert.True(once.IsCompleted);
    }
}
