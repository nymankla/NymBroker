using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Factory;
using NymBroker.Core.Idempotency;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;

namespace NymBroker.Tests;

/// <summary>
/// The <see cref="IExpiringIdempotencyStore"/> contract, run against each database store (the <c>NymBroker.Idempotency.*</c>
/// packages). A subclass supplies the store, skips when its database is not available, and cleans up its table.
/// </summary>
public abstract class IdempotencyStoreContractTests : IAsyncLifetime
{
    protected static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>A table unique to this test instance, so tests can run in parallel against one database.</summary>
    protected string TableName { get; } = $"nb_idem_{Guid.NewGuid():N}";

    /// <summary>Skips the test unless the database is available.</summary>
    protected abstract void RequireDatabase();

    /// <summary>
    /// A new store on <see cref="TableName"/> with <c>CleanupBatchSize = 2</c>. <paramref name="shared"/> is true when the
    /// test uses several stores or brokers on one table (SQLite then uses a file instead of <c>:memory:</c>).
    /// </summary>
    protected abstract IExpiringIdempotencyStore CreateStore(TimeSpan ttl, TimeSpan lease, bool shared);

    /// <summary>Registers the store on the shared <see cref="TableName"/> through the package's builder extension.</summary>
    protected abstract NymBrokerBuilder AddStore(NymBrokerBuilder builder);

    /// <summary>Rows in <see cref="TableName"/> of the shared database.</summary>
    protected abstract Task<long> CountRowsAsync();

    /// <summary>Drops the table (or deletes the database file).</summary>
    protected abstract ValueTask DropTableAsync();

    /// <summary>An open connection to the shared database, as the application's consumer would use (<see cref="IInbox"/>).</summary>
    protected abstract Task<DbConnection> OpenSharedConnectionAsync();

    private readonly List<IExpiringIdempotencyStore> _stores = [];

    /// <summary>A store disposed with the test; one hour TTL and five minute lease unless given.</summary>
    protected IExpiringIdempotencyStore Store(TimeSpan? ttl = null, TimeSpan? lease = null, bool shared = false)
    {
        var store = CreateStore(ttl ?? TimeSpan.FromHours(1), lease ?? TimeSpan.FromMinutes(5), shared);
        _stores.Add(store);
        return store;
    }

    [Fact]
    public async Task Claim_InProgress_Complete_Duplicate()
    {
        RequireDatabase();
        var store = Store();
        var id = Guid.NewGuid();

        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(id, Ct));
        Assert.Equal(IdempotencyClaimResult.InProgress, await store.TryClaimAsync(id, Ct));
        await store.CompleteAsync(id, Ct);
        Assert.Equal(IdempotencyClaimResult.Duplicate, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task Release_AllowsClaimAgain_ButDoesNotForgetACompletedMessage()
    {
        RequireDatabase();
        var store = Store();
        var id = Guid.NewGuid();

        await store.TryClaimAsync(id, Ct);
        await store.ReleaseAsync(id, Ct);
        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(id, Ct));

        await store.CompleteAsync(id, Ct);
        await store.ReleaseAsync(id, Ct);
        Assert.Equal(IdempotencyClaimResult.Duplicate, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task Complete_WithoutRow_StillRecordsTheMessage()
    {
        RequireDatabase();
        var store = Store();
        var id = Guid.NewGuid();

        await store.CompleteAsync(id, Ct);   // e.g. the lease expired and cleanup removed the claim

        Assert.Equal(IdempotencyClaimResult.Duplicate, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task ExpiredLease_CanBeTakenOver()
    {
        RequireDatabase();
        var store = Store(lease: TimeSpan.FromSeconds(1));
        var id = Guid.NewGuid();

        await store.TryClaimAsync(id, Ct);
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);

        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task ExpiredTtl_AllowsReprocessing()
    {
        RequireDatabase();
        var store = Store(ttl: TimeSpan.FromSeconds(1));
        var id = Guid.NewGuid();

        await store.TryClaimAsync(id, Ct);
        await store.CompleteAsync(id, Ct);
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);

        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task DeleteExpired_RemovesOnlyExpiredRows_InBatches()
    {
        RequireDatabase();
        var shortLived = Store(ttl: TimeSpan.FromSeconds(1), shared: true);
        var longLived = Store(shared: true);
        foreach (var _ in Enumerable.Range(0, 5))
            await shortLived.CompleteAsync(Guid.NewGuid(), Ct);
        var kept = Guid.NewGuid();
        await longLived.CompleteAsync(kept, Ct);
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);

        Assert.Equal(5, await longLived.DeleteExpiredAsync(Ct));   // CleanupBatchSize = 2 → three statements
        Assert.Equal(1L, await CountRowsAsync());
        Assert.Equal(IdempotencyClaimResult.Duplicate, await longLived.TryClaimAsync(kept, Ct));
    }

    [Fact]
    public async Task AutoCreate_IsIdempotent_AcrossStores()
    {
        RequireDatabase();
        var id = Guid.NewGuid();

        Assert.Equal(IdempotencyClaimResult.Claimed, await Store(shared: true).TryClaimAsync(id, Ct));
        Assert.Equal(IdempotencyClaimResult.InProgress, await Store(shared: true).TryClaimAsync(id, Ct));   // second store, same table
    }

    [Fact]
    public async Task ConcurrentClaims_ExactlyOneWins()
    {
        RequireDatabase();
        var stores = Enumerable.Range(0, 4).Select(_ => Store(shared: true)).ToList();
        await stores[0].ReleaseAsync(Guid.NewGuid(), Ct);   // create the table up front
        var id = Guid.NewGuid();

        var results = await Task.WhenAll(Enumerable.Range(0, 16)
            .Select(i => Task.Run(() => stores[i % stores.Count].TryClaimAsync(id, Ct).AsTask(), Ct)));

        Assert.Single(results, r => r == IdempotencyClaimResult.Claimed);
        Assert.All(results.Where(r => r != IdempotencyClaimResult.Claimed), r => Assert.Equal(IdempotencyClaimResult.InProgress, r));
    }

    [Fact]
    public async Task CompleteInTransaction_IsRecordedOnCommit_AndNotOnRollback()
    {
        RequireDatabase();
        var store = Assert.IsType<ITransactionalIdempotencyStore>(Store(shared: true), exactMatch: false);
        var committed = Guid.NewGuid();
        var rolledBack = Guid.NewGuid();
        await store.TryClaimAsync(committed, Ct);
        await store.TryClaimAsync(rolledBack, Ct);

        await using (var connection = await OpenSharedConnectionAsync())
        {
            await using (var transaction = await connection.BeginTransactionAsync(Ct))
            {
                await store.CompleteInTransactionAsync(committed, transaction, Ct);
                await transaction.CommitAsync(Ct);
            }
            await using (var transaction = await connection.BeginTransactionAsync(Ct))
            {
                await store.CompleteInTransactionAsync(rolledBack, transaction, Ct);
                await transaction.RollbackAsync(Ct);
            }
        }

        Assert.Equal(IdempotencyClaimResult.Duplicate, await store.TryClaimAsync(committed, Ct));
        await store.ReleaseAsync(committed, Ct);   // what the broker does after a failure: a completed row stays
        Assert.Equal(IdempotencyClaimResult.Duplicate, await store.TryClaimAsync(committed, Ct));

        Assert.Equal(IdempotencyClaimResult.InProgress, await store.TryClaimAsync(rolledBack, Ct));
        await store.ReleaseAsync(rolledBack, Ct);
        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(rolledBack, Ct));
    }

    // --- End to end: two brokers sharing one table ---

    [MessageName("tests.idempotency.contract.order")]
    public sealed class SharedOrder { public Guid Marker { get; set; } = Guid.NewGuid(); }

    public sealed class SlowOrderConsumer : IConsume<SharedOrder>
    {
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<Guid, int> Calls = new();
        public static int CallsFor(Guid marker) => Calls.GetValueOrDefault(marker);

        public async Task ConsumeAsync(SharedOrder message, IMessageContext context, CancellationToken ct = default)
        {
            Calls.AddOrUpdate(message.Marker, 1, (_, n) => n + 1);
            await Task.Delay(300, ct);   // keep the claim open while the other broker receives the same message
        }
    }

    [Fact]
    public async Task TwoBrokersOnOneTable_SameMessageDeliveredToBoth_IsConsumedOnce()
    {
        RequireDatabase();

        async Task<(ServiceProvider Provider, NymBrokerImpl Broker)> BuildAsync()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            AddStore(services.AddNymBroker().AddConsumer<SlowOrderConsumer>()).Build();
            var provider = services.BuildServiceProvider();
            var broker = provider.GetRequiredService<NymBrokerImpl>();
            await broker.StartAsync(Ct);
            return (provider, broker);
        }

        var (sp1, broker1) = await BuildAsync();
        var (sp2, broker2) = await BuildAsync();
        try
        {
            var order = new SharedOrder();
            string json;
            using (var stream = sp1.GetRequiredService<MessageSerializerJson>().Serialize(new MessageContext<SharedOrder> { Message = order }))
            using (var reader = new StreamReader(stream))
                json = await reader.ReadToEndAsync(Ct);

            var results = await Task.WhenAll(broker1.ProcessAsync(json, "In", Ct), broker2.ProcessAsync(json, "In", Ct));
            var redelivered = await broker2.ProcessAsync(json, "In", Ct);   // e.g. the loser's transport retried

            Assert.Equal(1, SlowOrderConsumer.CallsFor(order.Marker));
            Assert.Contains(results, r => r.Outcome == ProcessOutcome.Completed);
            Assert.Equal(ProcessOutcome.Completed, redelivered.Outcome);
        }
        finally
        {
            await broker1.StopAsync(Ct);
            await broker2.StopAsync(Ct);
            await sp1.DisposeAsync();
            await sp2.DisposeAsync();
        }
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
