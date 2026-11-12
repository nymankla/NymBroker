using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;

namespace NymBroker.Tests;

public sealed class DeferredQueueTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private sealed class Harness
    {
        public ConcurrentQueue<(int Item, DateTimeOffset At)> Delivered { get; } = new();
        public ConcurrentQueue<int> Dropped { get; } = new();
        public CapturingLogger<DeferredQueueTests> Logger { get; } = new();
        public Func<int, CancellationToken, Task>? Deliver { get; init; }

        public DeferredQueue<int> Create() => new(
            Deliver ?? ((item, _) =>
            {
                Delivered.Enqueue((item, DateTimeOffset.UtcNow));
                return Task.CompletedTask;
            }),
            Dropped.Enqueue, Logger, "test");
    }

    [Fact]
    public async Task ItemsAreDeliveredInDueOrder_NeverEarly()
    {
        var harness = new Harness();
        var queue = harness.Create();
        var start = DateTimeOffset.UtcNow;
        var due = new Dictionary<int, DateTimeOffset>
        {
            [1] = start.AddMilliseconds(300),
            [2] = start.AddMilliseconds(100),
            [3] = start.AddMilliseconds(200),
        };

        foreach (var (item, at) in due) Assert.True(queue.TryAdd(item, at));

        await DeferredContract.WaitUntilAsync(() => harness.Delivered.Count == 3, Timeout);
        Assert.Equal([2, 3, 1], harness.Delivered.Select(d => d.Item));
        Assert.All(harness.Delivered, d => Assert.True(d.At >= due[d.Item], $"Item {d.Item} delivered at {d.At:O}, due at {due[d.Item]:O}."));
        await queue.StopAsync(Timeout);
    }

    [Fact]
    public async Task EarlierItemAddedLater_WakesTheLoop()
    {
        var harness = new Harness();
        var queue = harness.Create();

        queue.TryAdd(1, DateTimeOffset.UtcNow.AddHours(1));
        await DeferredContract.WaitUntilAsync(() => queue.IsRunning, Timeout);
        await Task.Delay(50, Ct);   // the loop is now waiting for the item an hour away
        queue.TryAdd(2, DateTimeOffset.UtcNow.AddMilliseconds(50));

        await DeferredContract.WaitUntilAsync(() => harness.Delivered.Count == 1, Timeout);
        Assert.Equal(2, harness.Delivered.Single().Item);
        Assert.Equal(1, queue.WaitingCount);
        await queue.StopAsync(Timeout);
    }

    [Fact]
    public async Task ManyItems_OneLoop_EachDeliveredOnce()
    {
        var harness = new Harness();
        var queue = harness.Create();
        var start = DateTimeOffset.UtcNow;

        for (var i = 0; i < 5_000; i++)
            queue.TryAdd(i, start.AddMilliseconds(50 + i % 200));
        Assert.Equal(5_000, queue.WaitingCount);

        await DeferredContract.WaitUntilAsync(() => harness.Delivered.Count == 5_000, Timeout);
        Assert.Equal(5_000, harness.Delivered.Select(d => d.Item).Distinct().Count());
        Assert.Equal(0, queue.WaitingCount);
        await queue.StopAsync(Timeout);
    }

    [Fact]
    public async Task SlowDelivery_DoesNotHoldUpTheNextDueItem()
    {
        var release = new TaskCompletionSource();
        var delivered = new ConcurrentQueue<int>();
        var harness = new Harness
        {
            Deliver = async (item, _) =>
            {
                if (item == 1) await release.Task;
                delivered.Enqueue(item);
            }
        };
        var queue = harness.Create();

        queue.TryAdd(1, DateTimeOffset.UtcNow.AddMilliseconds(20));
        queue.TryAdd(2, DateTimeOffset.UtcNow.AddMilliseconds(100));

        await DeferredContract.WaitUntilAsync(() => delivered.Contains(2), Timeout);
        release.SetResult();
        await DeferredContract.WaitUntilAsync(() => delivered.Count == 2, Timeout);
        await queue.StopAsync(Timeout);
    }

    [Fact]
    public async Task Stop_DropsWaitingItems_RefusesNewOnes_UntilRestart()
    {
        var harness = new Harness();
        var queue = harness.Create();
        queue.TryAdd(1, DateTimeOffset.UtcNow.AddHours(1));
        queue.TryAdd(2, DateTimeOffset.UtcNow.AddHours(2));

        Assert.True(await queue.StopAsync(Timeout));

        Assert.Equal([1, 2], harness.Dropped.Order());
        Assert.False(queue.IsRunning);
        Assert.False(queue.TryAdd(3, DateTimeOffset.UtcNow.AddMilliseconds(10)));

        queue.Restart();
        Assert.True(queue.TryAdd(4, DateTimeOffset.UtcNow.AddMilliseconds(10)));
        await DeferredContract.WaitUntilAsync(() => harness.Delivered.Count == 1, Timeout);
        Assert.Equal(4, harness.Delivered.Single().Item);
        await queue.StopAsync(Timeout);
    }

    [Fact]
    public async Task Stop_CancelsADeliveryUnderWay_AndReportsOneThatIgnoresIt()
    {
        var cancelled = new TaskCompletionSource();
        var harness = new Harness
        {
            Deliver = async (_, ct) =>
            {
                try { await Task.Delay(System.Threading.Timeout.Infinite, ct); }
                catch (OperationCanceledException) { cancelled.SetResult(); }
            }
        };
        var queue = harness.Create();
        queue.TryAdd(1, DateTimeOffset.UtcNow);
        await DeferredContract.WaitUntilAsync(() => queue.WaitingCount == 0, Timeout);
        await Task.Delay(50, Ct);

        Assert.True(await queue.StopAsync(Timeout));
        Assert.True(cancelled.Task.IsCompleted);

        var stuck = new Harness { Deliver = (_, _) => Task.Delay(System.Threading.Timeout.Infinite, CancellationToken.None) };
        var stuckQueue = stuck.Create();
        stuckQueue.TryAdd(1, DateTimeOffset.UtcNow);
        await DeferredContract.WaitUntilAsync(() => stuckQueue.WaitingCount == 0, Timeout);
        Assert.False(await stuckQueue.StopAsync(TimeSpan.FromMilliseconds(100)));
    }

    [Fact]
    public async Task FailingDelivery_IsLogged()
    {
        var harness = new Harness { Deliver = (_, _) => throw new InvalidOperationException("boom") };
        var queue = harness.Create();

        queue.TryAdd(1, DateTimeOffset.UtcNow);

        await DeferredContract.WaitUntilAsync(() => harness.Logger.Entries.Any(e => e.Level == LogLevel.Error && e.Exception is not null), Timeout);
        Assert.True(await queue.StopAsync(Timeout));
    }
}
