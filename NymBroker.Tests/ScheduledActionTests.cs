using NymBroker.Core.Aggregator;
using NymBroker.Core.Impl;
using NymBroker.Core.PubSub;
using NymBroker.Core.Serialize;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace NymBroker.Tests;

/// <summary>Scheduled actions added after <c>StartAsync</c> (#50) and actions that throw (#51).</summary>
public sealed class ScheduledActionTests
{
    private static readonly TimeSpan Interval = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    private static (NymBrokerImpl Broker, CapturingLogger<NymBrokerImpl> Logger) BuildBroker()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<MessageSerializerJson>();
        services.AddSingleton<IAggregator, AggregatorImpl>();
        var sp = services.BuildServiceProvider();

        var logger = new CapturingLogger<NymBrokerImpl>();
        var broker = new NymBrokerImpl(
            sp.GetRequiredService<MessageSerializerJson>(),
            sp.GetRequiredService<IAggregator>(),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            logger);
        return (broker, logger);
    }

    /// <summary>Counts runs and lets a test wait until a given number of runs has happened.</summary>
    private sealed class TickCounter
    {
        private int _count;
        public int Count => Volatile.Read(ref _count);

        public void Tick() => Interlocked.Increment(ref _count);

        public async Task<bool> WaitForAsync(int atLeast, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow + timeout;
            while (Count < atLeast)
            {
                if (DateTime.UtcNow > deadline)
                    return false;
                await Task.Delay(10, TestContext.Current.CancellationToken);
            }
            return true;
        }
    }

    private static async Task AssertNoMoreTicksAsync(TickCounter counter)
    {
        var afterStop = counter.Count;
        await Task.Delay(Interval * 4, TestContext.Current.CancellationToken);
        Assert.Equal(afterStop, counter.Count);
    }

    [Fact]
    public async Task IntervalAction_AddedAfterStart_RunsAndIsStoppedByStopAsync()
    {
        var (broker, _) = BuildBroker();
        var counter = new TickCounter();

        await broker.StartAsync(TestContext.Current.CancellationToken);
        broker.AddScheduledAction(Interval, counter.Tick);

        Assert.True(await counter.WaitForAsync(2, WaitTimeout), "action added after StartAsync never ran");

        await broker.StopAsync(TestContext.Current.CancellationToken);
        await AssertNoMoreTicksAsync(counter);
    }

    [Fact]
    public async Task IntervalActionWithParameters_AddedAfterStart_Runs()
    {
        var (broker, _) = BuildBroker();
        var one = new TickCounter();
        var two = new TickCounter();

        await broker.StartAsync(TestContext.Current.CancellationToken);
        broker.AddScheduledAction(Interval, c => c.Tick(), one);
        broker.AddScheduledAction(Interval, (a, b) => { a.Tick(); b.Tick(); }, one, two);

        Assert.True(await one.WaitForAsync(2, WaitTimeout));
        Assert.True(await two.WaitForAsync(1, WaitTimeout));

        await broker.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task CronAction_AddedAfterStart_IsStartedAndStoppedByStopAsync()
    {
        var (broker, logger) = BuildBroker();

        await broker.StartAsync(TestContext.Current.CancellationToken);
        broker.AddScheduledAction("* * * * *", _ => { }, new object());

        // Cron resolution is one minute, so check that it was started rather than waiting for a run.
        Assert.Contains(logger.Entries, e => e.Message.Contains("started immediately", StringComparison.Ordinal));

        var stop = broker.StopAsync(TestContext.Current.CancellationToken);
        Assert.Same(stop, await Task.WhenAny(stop, Task.Delay(WaitTimeout, TestContext.Current.CancellationToken)));
        await stop;
    }

    [Fact]
    public async Task Action_AddedBeforeStart_IsNotStartedEarly()
    {
        var (broker, logger) = BuildBroker();
        var counter = new TickCounter();

        broker.AddScheduledAction(Interval, counter.Tick);
        await Task.Delay(Interval * 4, TestContext.Current.CancellationToken);

        Assert.Equal(0, counter.Count);
        Assert.DoesNotContain(logger.Entries, e => e.Message.Contains("started immediately", StringComparison.Ordinal));

        await broker.StartAsync(TestContext.Current.CancellationToken);
        Assert.True(await counter.WaitForAsync(1, WaitTimeout));
        await broker.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Action_AddedAfterStop_DoesNotRunUntilRestarted()
    {
        var (broker, _) = BuildBroker();
        var counter = new TickCounter();

        await broker.StartAsync(TestContext.Current.CancellationToken);
        await broker.StopAsync(TestContext.Current.CancellationToken);

        broker.AddScheduledAction(Interval, counter.Tick);
        await Task.Delay(Interval * 4, TestContext.Current.CancellationToken);
        Assert.Equal(0, counter.Count);

        await broker.StartAsync(TestContext.Current.CancellationToken);
        Assert.True(await counter.WaitForAsync(1, WaitTimeout));
        await broker.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task ThrowingAction_KeepsRunning_LogsError_AndStopAsyncDoesNotThrow()
    {
        var (broker, logger) = BuildBroker();
        var counter = new TickCounter();

        broker.AddScheduledAction(Interval, () =>
        {
            counter.Tick();
            if (counter.Count == 1)
                throw new InvalidOperationException("boom");
        });

        await broker.StartAsync(TestContext.Current.CancellationToken);

        Assert.True(await counter.WaitForAsync(3, WaitTimeout), "schedule stopped after the action threw");
        Assert.Contains(logger.Entries, e =>
            e.Level == LogLevel.Error && e.Exception is InvalidOperationException { Message: "boom" });

        await broker.StopAsync(TestContext.Current.CancellationToken);   // must not rethrow "boom"
        await AssertNoMoreTicksAsync(counter);
    }

    [Fact]
    public async Task ActionThatAlwaysThrows_AddedAfterStart_KeepsRunning_AndStopAsyncDoesNotThrow()
    {
        var (broker, logger) = BuildBroker();
        var counter = new TickCounter();

        await broker.StartAsync(TestContext.Current.CancellationToken);
        broker.AddScheduledAction(Interval, () =>
        {
            counter.Tick();
            throw new OperationCanceledException("the action's own timeout, not a broker shutdown");
        });

        Assert.True(await counter.WaitForAsync(3, WaitTimeout));
        Assert.True(logger.Entries.Count(e => e.Level == LogLevel.Error && e.Exception is OperationCanceledException) >= 2);

        await broker.StopAsync(TestContext.Current.CancellationToken);
        Assert.DoesNotContain(logger.Entries, e => e.Level == LogLevel.Critical);
    }

    [Fact]
    public async Task StopStartCycle_RestartsActionsAddedBeforeAndAfterStart()
    {
        var (broker, _) = BuildBroker();
        var before = new TickCounter();
        var after = new TickCounter();

        broker.AddScheduledAction(Interval, before.Tick);
        await broker.StartAsync(TestContext.Current.CancellationToken);
        broker.AddScheduledAction(Interval, after.Tick);

        Assert.True(await before.WaitForAsync(1, WaitTimeout));
        Assert.True(await after.WaitForAsync(1, WaitTimeout));

        await broker.StopAsync(TestContext.Current.CancellationToken);
        await AssertNoMoreTicksAsync(before);
        await AssertNoMoreTicksAsync(after);

        var beforeCount = before.Count;
        var afterCount = after.Count;
        await broker.StartAsync(TestContext.Current.CancellationToken);

        Assert.True(await before.WaitForAsync(beforeCount + 2, WaitTimeout), "action added before start was not restarted");
        Assert.True(await after.WaitForAsync(afterCount + 2, WaitTimeout), "action added after start was not restarted");

        await broker.StopAsync(TestContext.Current.CancellationToken);
        await AssertNoMoreTicksAsync(before);
        await AssertNoMoreTicksAsync(after);
    }

    [Fact]
    public async Task AddScheduledAction_ConcurrentWithStartAndStop_EveryActionIsStoppedByStopAsync()
    {
        var (broker, _) = BuildBroker();
        var counters = Enumerable.Range(0, 20).Select(_ => new TickCounter()).ToArray();

        var start = broker.StartAsync(TestContext.Current.CancellationToken);
        var adds = Task.WhenAll(counters.Select(c => Task.Run(() => broker.AddScheduledAction(Interval, c.Tick), TestContext.Current.CancellationToken)));
        await Task.WhenAll(start, adds);

        foreach (var c in counters)
            Assert.True(await c.WaitForAsync(1, WaitTimeout));

        await broker.StopAsync(TestContext.Current.CancellationToken);
        var snapshot = counters.Select(c => c.Count).ToArray();
        await Task.Delay(Interval * 4, TestContext.Current.CancellationToken);
        Assert.Equal(snapshot, counters.Select(c => c.Count).ToArray());
    }
}
