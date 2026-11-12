using System.Data.Common;
using Microsoft.Data.Sqlite;
using NymBroker.Core.Outbox;

namespace NymBroker.Tests;

/// <summary>
/// The <see cref="IOutboxStore"/> contract, run against each database (the <c>NymBroker.Outbox.*</c> packages). A subclass
/// supplies the store and a connection to its database, skips when the database is not available, and drops its table.
/// </summary>
public abstract class OutboxStoreContractTests : IAsyncLifetime
{
    protected static CancellationToken Ct => TestContext.Current.CancellationToken;
    protected static readonly TimeSpan Lease = TimeSpan.FromMinutes(5);

    /// <summary>A table unique to this test instance, so tests can run in parallel against one database.</summary>
    protected string TableName { get; } = $"nb_outbox_{Guid.NewGuid():N}";

    /// <summary>Skips the test unless the database is available.</summary>
    protected abstract void RequireDatabase();

    /// <summary>A new store on <see cref="TableName"/>.</summary>
    protected abstract IOutboxStore CreateStore();

    /// <summary>An open connection to the store's database, as the application would use.</summary>
    protected abstract Task<DbConnection> OpenConnectionAsync();

    /// <summary>Drops the table (or deletes the database file).</summary>
    protected abstract ValueTask DropTableAsync();

    /// <summary>
    /// SQLite has one writer at a time: a claim waits for an open application transaction instead of skipping its rows,
    /// so the test of skipping does not apply.
    /// </summary>
    protected virtual bool ClaimSkipsOpenTransactions => true;

    private readonly List<IOutboxStore> _stores = [];

    protected IOutboxStore Store()
    {
        var store = CreateStore();
        _stores.Add(store);
        return store;
    }

    /// <summary>Writes <paramref name="messages"/> in one application transaction and commits it.</summary>
    protected async Task AddCommittedAsync(IOutboxStore store, params OutboxMessage[] messages)
    {
        await using var connection = await OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync(Ct);
        foreach (var message in messages)
            await store.AddAsync(transaction, message, Ct);
        await transaction.CommitAsync(Ct);
    }

    protected static OutboxMessage Message(string body = "{}", string endpoint = "Out", DateTimeOffset? deliverAt = null)
        => new(endpoint, System.Text.Encoding.UTF8.GetBytes(body), deliverAt);

    [Fact]
    public async Task Committed_IsClaimedOnce_WithItsData()
    {
        RequireDatabase();
        var store = Store();
        await AddCommittedAsync(store, Message("""{"n":1}""", "Orders"));

        var claimed = Assert.Single(await store.ClaimAsync(Guid.NewGuid(), 10, Lease, Ct));
        Assert.Equal("Orders", claimed.EndpointName);
        Assert.Equal("""{"n":1}"""u8.ToArray(), claimed.Body);
        Assert.Null(claimed.DeliverAt);
        Assert.Equal(1, claimed.Attempt);
        Assert.Empty(await store.ClaimAsync(Guid.NewGuid(), 10, Lease, Ct));   // leased
    }

    [Fact]
    public async Task RolledBack_IsNeverClaimed()
    {
        RequireDatabase();
        var store = Store();
        await using (var connection = await OpenConnectionAsync())
        await using (var transaction = await connection.BeginTransactionAsync(Ct))
        {
            await store.AddAsync(transaction, Message(), Ct);
            await transaction.RollbackAsync(Ct);
        }

        Assert.Empty(await store.ClaimAsync(Guid.NewGuid(), 10, Lease, Ct));
    }

    [Fact]
    public async Task OpenTransaction_IsSkipped_ThenClaimedOnceCommitted()
    {
        RequireDatabase();
        Assert.SkipUnless(ClaimSkipsOpenTransactions, "SQLite has one writer at a time.");
        var store = Store();
        await AddCommittedAsync(store);   // creates the table outside the open transaction

        await using var connection = await OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync(Ct);
        await store.AddAsync(transaction, Message("open"), Ct);

        var claim = store.ClaimAsync(Guid.NewGuid(), 10, Lease, Ct);
        Assert.Empty(await claim.WaitAsync(TimeSpan.FromSeconds(5), Ct));   // skipped, not waited for

        await transaction.CommitAsync(Ct);
        Assert.Equal("open"u8.ToArray(), Assert.Single(await store.ClaimAsync(Guid.NewGuid(), 10, Lease, Ct)).Body);
    }

    [Fact]
    public async Task Claim_ReturnsOldestFirst_UpToTheBatchSize()
    {
        RequireDatabase();
        var store = Store();
        await AddCommittedAsync(store, [.. Enumerable.Range(0, 5).Select(i => Message($"{i}"))]);

        var first = await store.ClaimAsync(Guid.NewGuid(), 3, Lease, Ct);
        var rest = await store.ClaimAsync(Guid.NewGuid(), 3, Lease, Ct);

        Assert.Equal(["0", "1", "2"], first.Select(Body));
        Assert.Equal(["3", "4"], rest.Select(Body));
        Assert.True(first.Zip(first.Skip(1)).All(p => p.First.Id < p.Second.Id));
    }

    [Fact]
    public async Task ExpiredLease_IsClaimedAgain_AsTheNextAttempt()
    {
        RequireDatabase();
        var store = Store();
        await AddCommittedAsync(store, Message());

        await store.ClaimAsync(Guid.NewGuid(), 10, TimeSpan.FromSeconds(1), Ct);   // the dispatcher dies here
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);

        Assert.Equal(2, Assert.Single(await store.ClaimAsync(Guid.NewGuid(), 10, Lease, Ct)).Attempt);
    }

    [Fact]
    public async Task MarkSent_RemovesTheRow()
    {
        RequireDatabase();
        var store = Store();
        await AddCommittedAsync(store, Message());
        var claimId = Guid.NewGuid();
        var claimed = Assert.Single(await store.ClaimAsync(claimId, 10, TimeSpan.FromSeconds(1), Ct));

        await store.MarkSentAsync(claimed.Id, claimId, Ct);
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);

        Assert.Empty(await store.ClaimAsync(Guid.NewGuid(), 10, Lease, Ct));
    }

    [Fact]
    public async Task RetryLater_IsDueAfterTheDelay()
    {
        RequireDatabase();
        var store = Store();
        await AddCommittedAsync(store, Message());
        var claimId = Guid.NewGuid();
        var claimed = Assert.Single(await store.ClaimAsync(claimId, 10, Lease, Ct));

        await store.RetryLaterAsync(claimed.Id, claimId, TimeSpan.FromSeconds(1), "IOException: down", Ct);

        Assert.Empty(await store.ClaimAsync(Guid.NewGuid(), 10, Lease, Ct));
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);
        Assert.Equal(2, Assert.Single(await store.ClaimAsync(Guid.NewGuid(), 10, Lease, Ct)).Attempt);
    }

    [Fact]
    public async Task MarkFailed_IsNeverClaimedAgain()
    {
        RequireDatabase();
        var store = Store();
        await AddCommittedAsync(store, Message());
        var claimId = Guid.NewGuid();
        var claimed = Assert.Single(await store.ClaimAsync(claimId, 10, TimeSpan.FromSeconds(1), Ct));

        await store.MarkFailedAsync(claimed.Id, claimId, "gave up", Ct);
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);

        Assert.Empty(await store.ClaimAsync(Guid.NewGuid(), 10, Lease, Ct));
    }

    [Fact]
    public async Task Release_IsDueNow_AndTheAttemptDoesNotCount()
    {
        RequireDatabase();
        var store = Store();
        await AddCommittedAsync(store, Message());
        var claimId = Guid.NewGuid();
        var claimed = Assert.Single(await store.ClaimAsync(claimId, 10, Lease, Ct));

        await store.ReleaseAsync(claimed.Id, claimId, Ct);

        Assert.Equal(1, Assert.Single(await store.ClaimAsync(Guid.NewGuid(), 10, Lease, Ct)).Attempt);
    }

    [Fact]
    public async Task WritesWithAnOldClaim_ChangeNothing()
    {
        RequireDatabase();
        var store = Store();
        await AddCommittedAsync(store, Message());
        var staleClaim = Guid.NewGuid();
        await store.ClaimAsync(staleClaim, 10, TimeSpan.FromSeconds(1), Ct);
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);
        var current = Guid.NewGuid();
        var claimed = Assert.Single(await store.ClaimAsync(current, 10, TimeSpan.FromSeconds(1), Ct));

        // The dispatcher whose lease expired finishes late: none of its writes may touch the new claim.
        await store.MarkSentAsync(claimed.Id, staleClaim, Ct);
        await store.MarkFailedAsync(claimed.Id, staleClaim, "late", Ct);
        await store.ReleaseAsync(claimed.Id, staleClaim, Ct);
        Assert.Empty(await store.ClaimAsync(Guid.NewGuid(), 10, Lease, Ct));

        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);
        Assert.Equal(3, Assert.Single(await store.ClaimAsync(Guid.NewGuid(), 10, Lease, Ct)).Attempt);
    }

    [Fact]
    public async Task Deferred_IsNotClaimedBeforeItIsDue()
    {
        RequireDatabase();
        var store = Store();
        var deliverAt = DateTimeOffset.UtcNow.AddSeconds(2);
        await AddCommittedAsync(store, Message(deliverAt: deliverAt));

        Assert.Empty(await store.ClaimAsync(Guid.NewGuid(), 10, Lease, Ct));
        await Task.Delay(TimeSpan.FromSeconds(3), Ct);

        var claimed = Assert.Single(await store.ClaimAsync(Guid.NewGuid(), 10, Lease, Ct));
        Assert.NotNull(claimed.DeliverAt);
        Assert.True((claimed.DeliverAt.Value - deliverAt).Duration() < TimeSpan.FromMilliseconds(1), $"{claimed.DeliverAt:O} vs {deliverAt:O}");
    }

    [Fact]
    public async Task ConcurrentClaims_NeverReturnARowTwice()
    {
        RequireDatabase();
        var stores = Enumerable.Range(0, 4).Select(_ => Store()).ToList();
        await AddCommittedAsync(stores[0], [.. Enumerable.Range(0, 40).Select(i => Message($"{i}"))]);

        var claims = await Task.WhenAll(Enumerable.Range(0, 8)
            .Select(i => Task.Run(() => stores[i % stores.Count].ClaimAsync(Guid.NewGuid(), 10, Lease, Ct), Ct)));

        var ids = claims.SelectMany(c => c).Select(m => m.Id).ToList();
        Assert.Equal(40, ids.Count);
        Assert.Equal(40, ids.Distinct().Count());
    }

    [Fact]
    public async Task TransactionOfAnotherDatabase_IsRejected()
    {
        RequireDatabase();
        Assert.SkipWhen(this is SqliteOutboxStoreTests, "The other database used here is SQLite.");
        await using var other = new SqliteConnection("Data Source=:memory:");
        await other.OpenAsync(Ct);
        await using var transaction = await other.BeginTransactionAsync(Ct);

        await Assert.ThrowsAsync<ArgumentException>(() => Store().AddAsync(transaction, Message(), Ct));
    }

    private static string Body(ClaimedOutboxMessage m) => System.Text.Encoding.UTF8.GetString(m.Body);

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var store in _stores.OfType<IAsyncDisposable>())
            await store.DisposeAsync();
        await DropTableAsync();
        GC.SuppressFinalize(this);
    }
}
