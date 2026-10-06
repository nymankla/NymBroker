using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Idempotency;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;
using NymBroker.Idempotency.SqlServer;

namespace NymBroker.Tests;

/// <summary>
/// Tests for <see cref="SqlServerIdempotencyStore"/> (#55). Settings and registration tests always run; the rest need
/// a real SQL Server and are skipped unless <c>NYMBROKER_SQLSERVER_CS</c> is set (see <see cref="SqlServerEndPointTests"/>).
/// </summary>
public sealed class SqlServerIdempotencyStoreTests : IAsyncLifetime
{
    private const string ConnectionStringVariable = "NYMBROKER_SQLSERVER_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // Unique per test instance so tests can run in parallel against one database.
    private readonly string _tableName = $"dbo.nb_idem_{Guid.NewGuid():N}";

    private static void RequireSqlServer()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString),
            $"Set {ConnectionStringVariable} to run SQL Server integration tests.");

    private SqlServerIdempotencySettings Settings(TimeSpan? ttl = null, TimeSpan? lease = null) => new()
    {
        ConnectionString = ConnectionString!,
        TableName        = _tableName,
        Ttl              = ttl ?? TimeSpan.FromHours(1),
        LeaseTimeout     = lease ?? TimeSpan.FromMinutes(5),
        CleanupBatchSize = 2
    };

    private SqlServerIdempotencyStore CreateStore(TimeSpan? ttl = null, TimeSpan? lease = null)
        => new(Settings(ttl, lease), NullLogger<SqlServerIdempotencyStore>.Instance);

    // --- Settings and registration (always run) ---

    [Fact]
    public void InvalidSettings_ThrowAtRegistration()
    {
        Assert.Throws<ArgumentException>(() => new SqlServerIdempotencySettings { ConnectionString = " " }.Validate());
        Assert.Throws<ArgumentException>(() => new SqlServerIdempotencySettings { TableName = "dbo." }.Validate());
        Assert.Throws<ArgumentException>(() => new SqlServerIdempotencySettings { Ttl = TimeSpan.FromMilliseconds(500) }.Validate());
        Assert.Throws<ArgumentException>(() => new SqlServerIdempotencySettings { LeaseTimeout = TimeSpan.Zero }.Validate());
        Assert.Throws<ArgumentException>(() => new SqlServerIdempotencySettings { CleanupInterval = TimeSpan.FromSeconds(-1) }.Validate());
        Assert.Throws<ArgumentException>(() => new SqlServerIdempotencySettings { CleanupBatchSize = 0 }.Validate());

        Assert.Throws<ArgumentException>(() => new ServiceCollection().AddNymBroker()
            .AddSqlServerIdempotency(new SqlServerIdempotencySettings { TableName = "" }));
    }

    [Fact]
    public void Settings_RoundSecondsUp()
    {
        var s = new SqlServerIdempotencySettings { Ttl = TimeSpan.FromSeconds(1.2), LeaseTimeout = TimeSpan.FromSeconds(30) };
        Assert.Equal(2, s.TtlSeconds);
        Assert.Equal(30, s.LeaseSeconds);
    }

    [Fact]
    public void Sql_QuotesTableName()
    {
        var sql = SqlServerIdempotencySql.Claim("dbo.my]table");
        Assert.Contains("[dbo].[my]]table]", sql);
        Assert.Contains("UPDLOCK", sql);
    }

    [Fact]
    public async Task AddSqlServerIdempotency_RegistersStoreAndCleanupService_AndBrokerUsesIt()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker().AddSqlServerIdempotency(new SqlServerIdempotencySettings { TableName = "dbo.x" }).Build();
        await using var sp = services.BuildServiceProvider();

        Assert.IsType<SqlServerIdempotencyStore>(sp.GetRequiredService<IIdempotencyStore>());
        Assert.Contains(sp.GetServices<IHostedService>(), s => s is SqlServerIdempotencyCleanupService);
    }

    [Fact]
    public async Task AddSqlServerIdempotency_ZeroCleanupInterval_RegistersNoCleanupService()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker().AddSqlServerIdempotency(new SqlServerIdempotencySettings { CleanupInterval = TimeSpan.Zero }).Build();
        await using var sp = services.BuildServiceProvider();

        Assert.DoesNotContain(sp.GetServices<IHostedService>(), s => s is SqlServerIdempotencyCleanupService);
    }

    // --- Store behaviour (SQL Server) ---

    [Fact]
    public async Task Claim_InProgress_Complete_Duplicate()
    {
        RequireSqlServer();
        var store = CreateStore();
        var id = Guid.NewGuid();

        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(id, Ct));
        Assert.Equal(IdempotencyClaimResult.InProgress, await store.TryClaimAsync(id, Ct));
        await store.CompleteAsync(id, Ct);
        Assert.Equal(IdempotencyClaimResult.Duplicate, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task Release_AllowsClaimAgain_ButDoesNotForgetACompletedMessage()
    {
        RequireSqlServer();
        var store = CreateStore();
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
        RequireSqlServer();
        var store = CreateStore();
        var id = Guid.NewGuid();

        await store.CompleteAsync(id, Ct);   // e.g. the lease expired and cleanup removed the claim

        Assert.Equal(IdempotencyClaimResult.Duplicate, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task ExpiredLease_CanBeTakenOver()
    {
        RequireSqlServer();
        var store = CreateStore(lease: TimeSpan.FromSeconds(1));
        var id = Guid.NewGuid();

        await store.TryClaimAsync(id, Ct);
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);

        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task ExpiredTtl_AllowsReprocessing()
    {
        RequireSqlServer();
        var store = CreateStore(ttl: TimeSpan.FromSeconds(1));
        var id = Guid.NewGuid();

        await store.TryClaimAsync(id, Ct);
        await store.CompleteAsync(id, Ct);
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);

        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task DeleteExpired_RemovesOnlyExpiredRows_InBatches()
    {
        RequireSqlServer();
        var shortLived = CreateStore(ttl: TimeSpan.FromSeconds(1));
        var longLived = CreateStore();
        foreach (var _ in Enumerable.Range(0, 5))
            await shortLived.CompleteAsync(Guid.NewGuid(), Ct);
        var kept = Guid.NewGuid();
        await longLived.CompleteAsync(kept, Ct);
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);

        var deleted = await longLived.DeleteExpiredAsync(Ct);   // CleanupBatchSize = 2 → three statements

        Assert.Equal(5, deleted);
        Assert.Equal(1, await CountRowsAsync());
        Assert.Equal(IdempotencyClaimResult.Duplicate, await longLived.TryClaimAsync(kept, Ct));
    }

    [Fact]
    public async Task AutoCreate_IsIdempotent_AcrossStores()
    {
        RequireSqlServer();
        var id = Guid.NewGuid();

        Assert.Equal(IdempotencyClaimResult.Claimed, await CreateStore().TryClaimAsync(id, Ct));
        Assert.Equal(IdempotencyClaimResult.InProgress, await CreateStore().TryClaimAsync(id, Ct));   // second store, same table
    }

    [Fact]
    public async Task ConcurrentClaims_ExactlyOneWins()
    {
        RequireSqlServer();
        var stores = Enumerable.Range(0, 4).Select(_ => CreateStore()).ToList();
        await stores[0].ReleaseAsync(Guid.NewGuid(), Ct);   // create the table up front
        var id = Guid.NewGuid();

        var results = await Task.WhenAll(Enumerable.Range(0, 16)
            .Select(i => Task.Run(() => stores[i % stores.Count].TryClaimAsync(id, Ct).AsTask(), Ct)));

        Assert.Single(results, r => r == IdempotencyClaimResult.Claimed);
        Assert.All(results.Where(r => r != IdempotencyClaimResult.Claimed), r => Assert.Equal(IdempotencyClaimResult.InProgress, r));
    }

    // --- End to end: two brokers sharing one table ---

    [MessageName("tests.idempotency.sqlserver.order")]
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
        RequireSqlServer();
        var settings = Settings();

        async Task<(ServiceProvider Provider, NymBrokerImpl Broker)> BuildAsync()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddNymBroker().AddConsumer<SlowOrderConsumer>().AddSqlServerIdempotency(settings).Build();
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

    // --- Helpers ---

    private async Task<int> CountRowsAsync()
    {
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {_tableName}";
        return (int)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString)) return;
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DROP TABLE IF EXISTS {_tableName}";
        await cmd.ExecuteNonQueryAsync();
    }
}
