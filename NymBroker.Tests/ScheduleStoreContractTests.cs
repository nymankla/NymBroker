using Microsoft.Extensions.DependencyInjection;
using NymBroker.Core.DI;
using NymBroker.Core.Factory;
using NymBroker.Core.Impl;
using NymBroker.Core.Scheduling;

namespace NymBroker.Tests;

/// <summary>
/// The <see cref="IScheduleStore"/> contract, run against each database store (the <c>NymBroker.Scheduling.*</c>
/// packages). A subclass supplies the store, skips when its database is not available, and drops its table.
/// </summary>
public abstract class ScheduleStoreContractTests : IAsyncLifetime
{
    protected static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>A table unique to this test instance.</summary>
    protected string TableName { get; } = $"nb_sched_{Guid.NewGuid():N}";

    /// <summary>Skips the test unless the database is available.</summary>
    protected abstract void RequireDatabase();

    /// <summary>A new store instance on <see cref="TableName"/>.</summary>
    protected abstract IScheduleStore CreateStore();

    /// <summary>Registers the store on <see cref="TableName"/> through the package's builder extension.</summary>
    protected abstract NymBrokerBuilder AddStore(NymBrokerBuilder builder);

    protected abstract ValueTask DropTableAsync();

    private readonly List<IScheduleStore> _stores = [];

    private IScheduleStore Store()
    {
        var store = CreateStore();
        _stores.Add(store);
        return store;
    }

    private static DateTimeOffset At(int hour, int millisecond = 0)
        => new DateTimeOffset(2026, 10, 29, hour, 0, 0, TimeSpan.Zero).AddMilliseconds(millisecond);

    [Fact]
    public async Task UnknownName_HasNoLastRun()
    {
        RequireDatabase();
        Assert.Null(await Store().GetLastRunAsync("never-ran", Ct));
    }

    [Fact]
    public async Task Claim_RecordsEachOccurrenceOnce_AndOnlyMovesForward()
    {
        RequireDatabase();
        var store = Store();

        Assert.True(await store.TryClaimAsync("job", At(10, 123), Ct));
        Assert.Equal(At(10, 123), await store.GetLastRunAsync("job", Ct));

        Assert.False(await store.TryClaimAsync("job", At(10, 123), Ct));
        Assert.False(await store.TryClaimAsync("job", At(9), Ct));
        Assert.Equal(At(10, 123), await store.GetLastRunAsync("job", Ct));

        Assert.True(await store.TryClaimAsync("job", At(11), Ct));
        Assert.Equal(At(11), await store.GetLastRunAsync("job", Ct));
        Assert.Null(await store.GetLastRunAsync("other", Ct));
    }

    [Fact]
    public async Task Names_AreStoredAsIs()
    {
        RequireDatabase();
        var store = Store();
        string[] names = ["it's \"quoted\"; DROP TABLE x", "räksmörgås ✓", new string('n', 200)];

        foreach (var (name, i) in names.Select((n, i) => (n, i)))
            Assert.True(await store.TryClaimAsync(name, At(i + 1), Ct));
        foreach (var (name, i) in names.Select((n, i) => (n, i)))
            Assert.Equal(At(i + 1), await store.GetLastRunAsync(name, Ct));
    }

    [Fact]
    public async Task TwoStores_OnOneTable_CreateItAndShareRuns()
    {
        RequireDatabase();
        var first = Store();
        var second = Store();

        var created = await Task.WhenAll(first.TryClaimAsync("a", At(1), Ct).AsTask(), second.TryClaimAsync("b", At(2), Ct).AsTask());

        Assert.All(created, Assert.True);
        Assert.Equal(At(2), await first.GetLastRunAsync("b", Ct));
        Assert.Equal(At(1), await second.GetLastRunAsync("a", Ct));
    }

    [Fact]
    public async Task ConcurrentClaims_OfOneOccurrence_AcrossStores_ExactlyOneWins()
    {
        RequireDatabase();
        var stores = Enumerable.Range(0, 4).Select(_ => Store()).ToList();
        await stores[0].GetLastRunAsync("warm-up", Ct);   // create the table first; creation races are not under test

        foreach (var occurrence in new[] { At(10), At(11) })   // first claim inserts, second updates
        {
            var results = await Task.WhenAll(Enumerable.Range(0, 16)
                .Select(i => stores[i % stores.Count].TryClaimAsync("race", occurrence, Ct).AsTask()));
            Assert.Single(results, r => r);
        }
    }

    [Fact]
    public async Task Broker_WithTheStore_CatchesUpAMissedRunAfterARestart()
    {
        RequireDatabase();
        // The previous process ran the action two days ago, then stopped.
        Assert.True(await Store().TryClaimAsync("nightly", DateTimeOffset.UtcNow.AddDays(-2), Ct));

        var ran = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var services = new ServiceCollection().AddLogging();
        AddStore(services.AddNymBroker())
            .AddScheduledAction("nightly", "0 2 * * *", (_, _) => { ran.TrySetResult(); return Task.CompletedTask; },
                o => o.MissedRuns = MissedRunPolicy.RunOnce)
            .Build();
        await using var provider = services.BuildServiceProvider();
        var broker = provider.GetRequiredService<INymBroker>();

        await broker.StartAsync(Ct);
        await ran.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        await broker.StopAsync(Ct);

        Assert.True(await Store().GetLastRunAsync("nightly", Ct) > DateTimeOffset.UtcNow.AddDays(-1.1));
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var store in _stores.OfType<IAsyncDisposable>())
            await store.DisposeAsync();
        await DropTableAsync();
        GC.SuppressFinalize(this);
    }
}
