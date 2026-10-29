using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Aggregator;
using NymBroker.Core.DI;
using NymBroker.Core.Impl;
using NymBroker.Core.Scheduling;
using NymBroker.Core.Serialize;

namespace NymBroker.Tests;

/// <summary>Named scheduled actions: occurrences claimed in an <see cref="IScheduleStore"/>, and catching up missed runs.</summary>
public sealed class NamedScheduledActionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan Hour = TimeSpan.FromHours(1);

    private static DateTimeOffset Utc(int hour, int minute = 0, int second = 0)
        => new(2026, 10, 29, hour, minute, second, TimeSpan.Zero);

    private static (NymBrokerImpl Broker, CapturingLogger<NymBrokerImpl> Logger) BuildBroker(IScheduleStore store)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<MessageSerializerJson>();
        services.AddSingleton<IAggregator, AggregatorImpl>();
        var logger = new CapturingLogger<NymBrokerImpl>();
        var broker = TestBroker.Create(services.BuildServiceProvider(), logger: logger);
        broker.SetScheduleStore(store);
        return (broker, logger);
    }

    /// <summary>Counts runs; lets a test wait for a number of them.</summary>
    private sealed class RunCounter
    {
        private int _count;
        public int Count => Volatile.Read(ref _count);

        public Task RunAsync(CancellationToken _)
        {
            Interlocked.Increment(ref _count);
            return Task.CompletedTask;
        }

        public async Task<bool> WaitForAsync(int atLeast, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow + timeout;
            while (Count < atLeast)
            {
                if (DateTime.UtcNow > deadline)
                    return false;
                await Task.Delay(10, Ct);
            }
            return true;
        }
    }

    /// <summary>Wraps the in-memory store, counting claims and optionally failing them.</summary>
    private sealed class RecordingStore : IScheduleStore
    {
        private readonly InMemoryScheduleStore _inner = new();
        private int _won, _lost, _failuresLeft;

        public int Won => Volatile.Read(ref _won);
        public int Lost => Volatile.Read(ref _lost);

        public void FailNextClaims(int count) => Volatile.Write(ref _failuresLeft, count);

        public ValueTask<DateTimeOffset?> GetLastRunAsync(string name, CancellationToken ct = default) => _inner.GetLastRunAsync(name, ct);

        public async ValueTask<bool> TryClaimAsync(string name, DateTimeOffset occurrence, CancellationToken ct = default)
        {
            if (Interlocked.Decrement(ref _failuresLeft) >= 0)
                throw new InvalidOperationException("store is down");
            var claimed = await _inner.TryClaimAsync(name, occurrence, ct);
            Interlocked.Increment(ref claimed ? ref _won : ref _lost);
            return claimed;
        }
    }

    // --- Timing ---

    [Fact]
    public void GridTiming_OccurrencesAreMultiplesOfTheIntervalSinceTheEpoch()
    {
        var timing = new GridTiming(Hour);

        Assert.Equal(Utc(11), timing.Next(Utc(10, 20, 30)));
        Assert.Equal(Utc(11), timing.Next(Utc(10)));               // strictly after
        Assert.Equal(Utc(10), timing.LatestBetween(Utc(8, 30), Utc(10, 59)));
        Assert.Null(timing.LatestBetween(Utc(10), Utc(10, 59)));   // (after, atOrBefore]
        Assert.Equal(Utc(10), timing.LatestBetween(Utc(9, 59), Utc(10)));
    }

    [Fact]
    public void GridTiming_RejectsANonPositiveOrSubMillisecondInterval()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new GridTiming(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => new GridTiming(TimeSpan.FromTicks(15_000)));
    }

    [Fact]
    public void CronTiming_LatestBetween_ReturnsTheLastOccurrenceInTheRange()
    {
        var timing = new CronTiming(CronSchedule.Parse("*/15 * * * *"));
        var from = DateTimeOffset.Now.Date.AddHours(-12);

        Assert.Equal(from.AddMinutes(45), timing.LatestBetween(from, from.AddMinutes(50)));
        Assert.Null(timing.LatestBetween(from, from.AddMinutes(14)));
    }

    [Fact]
    public void FindMissed_TakesTheLatestOccurrenceSinceTheLastRun_WithinMaxCatchUp()
    {
        var timing = new GridTiming(Hour);

        Assert.Equal(Utc(10), timing.FindMissed(lastRun: Utc(7), now: Utc(10, 30), maxCatchUp: null));
        Assert.Null(timing.FindMissed(lastRun: Utc(10), now: Utc(10, 30), maxCatchUp: null));
        Assert.Equal(Utc(10), timing.FindMissed(lastRun: Utc(7), now: Utc(10, 30), maxCatchUp: TimeSpan.FromMinutes(45)));
        Assert.Null(timing.FindMissed(lastRun: Utc(7), now: Utc(10, 30), maxCatchUp: TimeSpan.FromMinutes(20)));
    }

    [Fact]
    public async Task DelayUntil_AnOccurrenceMonthsAway_WaitsInsteadOfThrowing()
    {
        // Task.Delay rejects delays over ~49 days, which ended the loop of an @yearly action.
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        cts.CancelAfter(TimeSpan.FromMilliseconds(100));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => NymBrokerImpl.DelayUntilAsync(DateTimeOffset.Now.AddDays(200), cts.Token));
    }

    // --- In-memory store ---

    [Fact]
    public async Task InMemoryStore_ClaimsEachOccurrenceOnce_AndOnlyMovesForward()
    {
        var store = new InMemoryScheduleStore();

        Assert.Null(await store.GetLastRunAsync("job", Ct));
        Assert.True(await store.TryClaimAsync("job", Utc(10), Ct));
        Assert.False(await store.TryClaimAsync("JOB", Utc(10), Ct));
        Assert.False(await store.TryClaimAsync("job", Utc(9), Ct));
        Assert.True(await store.TryClaimAsync("job", Utc(11), Ct));
        Assert.Equal(Utc(11), await store.GetLastRunAsync("job", Ct));
    }

    // --- Catching up ---

    [Fact]
    public async Task RunOnce_WithAMissedOccurrence_RunsOnceAtStart()
    {
        var store = new InMemoryScheduleStore();
        await store.TryClaimAsync("report", DateTimeOffset.UtcNow.AddHours(-3), Ct);
        var (broker, logger) = BuildBroker(store);
        var counter = new RunCounter();
        broker.AddNamedScheduledAction("report", new GridTiming(Hour), MissedRunPolicy.RunOnce, null, counter.RunAsync);

        await broker.StartAsync(Ct);
        Assert.True(await counter.WaitForAsync(1, WaitTimeout), "the missed run was not caught up");
        await Task.Delay(200, Ct);
        await broker.StopAsync(Ct);

        Assert.Equal(1, counter.Count);
        Assert.Equal(new GridTiming(Hour).LatestBetween(DateTimeOffset.UnixEpoch, DateTimeOffset.UtcNow), await store.GetLastRunAsync("report", Ct));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Information && e.Message.Contains("missed its run", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(MissedRunPolicy.Skip, -3.0, null)]           // policy is Skip
    [InlineData(MissedRunPolicy.RunOnce, null, null)]        // never ran: nothing known to be missed
    [InlineData(MissedRunPolicy.RunOnce, -3.0, 0.000001)]    // missed occurrence older than MaxCatchUp
    public async Task MissedOccurrence_IsNotRun(MissedRunPolicy policy, double? lastRunHoursAgo, double? maxCatchUpHours)
    {
        var store = new InMemoryScheduleStore();
        if (lastRunHoursAgo is { } ago)
            await store.TryClaimAsync("report", DateTimeOffset.UtcNow.AddHours(ago), Ct);
        var (broker, _) = BuildBroker(store);
        var counter = new RunCounter();
        broker.AddNamedScheduledAction("report", new GridTiming(Hour), policy,
            maxCatchUpHours is { } h ? TimeSpan.FromHours(h) : null, counter.RunAsync);

        await broker.StartAsync(Ct);
        await Task.Delay(300, Ct);
        await broker.StopAsync(Ct);

        Assert.Equal(0, counter.Count);
    }

    // --- Shared store ---

    [Fact]
    public async Task TwoBrokers_SharingAStore_RunEachOccurrenceOnce()
    {
        var store = new RecordingStore();
        var counter = new RunCounter();
        var (first, _) = BuildBroker(store);
        var (second, _) = BuildBroker(store);
        foreach (var broker in new[] { first, second })
            broker.AddNamedScheduledAction("tick", new GridTiming(TimeSpan.FromMilliseconds(100)), MissedRunPolicy.Skip, null, counter.RunAsync);

        await first.StartAsync(Ct);
        await second.StartAsync(Ct);
        Assert.True(await counter.WaitForAsync(5, WaitTimeout));
        await first.StopAsync(Ct);
        await second.StopAsync(Ct);

        Assert.Equal(store.Won, counter.Count);
        Assert.True(store.Lost >= 4, $"expected the other broker to lose most claims, lost {store.Lost}");
    }

    [Fact]
    public async Task StoreFailure_IsLogged_SkipsTheOccurrence_AndTheScheduleContinues()
    {
        var store = new RecordingStore();
        store.FailNextClaims(1);
        var (broker, logger) = BuildBroker(store);
        var counter = new RunCounter();
        broker.AddNamedScheduledAction("tick", new GridTiming(TimeSpan.FromMilliseconds(50)), MissedRunPolicy.Skip, null, counter.RunAsync);

        await broker.StartAsync(Ct);
        Assert.True(await counter.WaitForAsync(2, WaitTimeout));
        await broker.StopAsync(Ct);

        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error && e.Message.Contains("Could not claim", StringComparison.Ordinal)
                                             && e.Message.Contains("tick", StringComparison.Ordinal));
        Assert.Equal(store.Won, counter.Count);
        Assert.DoesNotContain(logger.Entries, e => e.Level == LogLevel.Critical);
    }

    [Fact]
    public async Task FailingAction_StillCountsAsRun()
    {
        var store = new InMemoryScheduleStore();
        var (broker, logger) = BuildBroker(store);
        var runs = new RunCounter();
        broker.AddNamedScheduledAction("fails", new GridTiming(TimeSpan.FromMilliseconds(50)), MissedRunPolicy.Skip, null, async ct =>
        {
            await runs.RunAsync(ct);
            throw new InvalidOperationException("boom");
        });

        await broker.StartAsync(Ct);
        Assert.True(await runs.WaitForAsync(2, WaitTimeout));
        await broker.StopAsync(Ct);

        Assert.NotNull(await store.GetLastRunAsync("fails", Ct));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error && e.Message.Contains("Scheduled action fails failed", StringComparison.Ordinal));
    }

    // --- Builder ---

    [Fact]
    public async Task Builder_NamedAction_UsesTheRegisteredStore_AndCatchesUp()
    {
        var store = new RecordingStore();
        await store.TryClaimAsync("nightly", DateTimeOffset.UtcNow.AddDays(-2), Ct);
        var counter = new RunCounter();
        var services = new ServiceCollection().AddLogging();
        services.AddNymBroker()
            .AddScheduleStore(_ => store)
            .AddScheduledAction("nightly", "0 2 * * *", (_, ct) => counter.RunAsync(ct), o => o.MissedRuns = MissedRunPolicy.RunOnce)
            .Build();
        await using var provider = services.BuildServiceProvider();
        var broker = provider.GetRequiredService<INymBroker>();

        await broker.StartAsync(Ct);
        Assert.True(await counter.WaitForAsync(1, WaitTimeout));
        await broker.StopAsync(Ct);

        Assert.Equal(2, store.Won);   // the seed and the catch-up
    }

    [Fact]
    public void Builder_InvalidNamedActions_ThrowWhenAdded()
    {
        var builder = new ServiceCollection().AddNymBroker();
        Func<IServiceProvider, CancellationToken, Task> action = (_, _) => Task.CompletedTask;

        builder.AddScheduledAction("job", Hour, action);
        Assert.Throws<ArgumentException>(() => builder.AddScheduledAction("JOB", "@daily", action));
        Assert.ThrowsAny<ArgumentException>(() => builder.AddScheduledAction(" ", Hour, action));
        Assert.Throws<ArgumentException>(() => builder.AddScheduledAction(new string('x', 201), Hour, action));
        Assert.Throws<ArgumentException>(() => builder.AddScheduledAction("tab\tname", Hour, action));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.AddScheduledAction("a", TimeSpan.Zero, action));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.AddScheduledAction("b", TimeSpan.FromTicks(1), action));
        Assert.ThrowsAny<FormatException>(() => builder.AddScheduledAction("c", "not a cron", action));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.AddScheduledAction("d", Hour, action, o => o.MaxCatchUp = TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.AddScheduledAction("e", Hour, action, o => o.MissedRuns = (MissedRunPolicy)7));
    }
}
