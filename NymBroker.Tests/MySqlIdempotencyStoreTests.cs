using MySqlConnector;
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
using NymBroker.Idempotency.MySql;

namespace NymBroker.Tests;

/// <summary>
/// Tests for <see cref="MySqlIdempotencyStore"/> (#76). Settings and registration tests always run; the rest need
/// a real MySQL and are skipped unless <c>NYMBROKER_MYSQL_CS</c> is set (see <see cref="MySqlEndPointTests"/>).
/// </summary>
public sealed class MySqlIdempotencyStoreTests : IAsyncLifetime
{
    private const string ConnectionStringVariable = "NYMBROKER_MYSQL_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // Unique per test instance so tests can run in parallel against one database.
    private readonly string _tableName = $"nb_idem_{Guid.NewGuid():N}";

    private static void RequireMySql()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString),
            $"Set {ConnectionStringVariable} to run MySQL integration tests (scripts/setup-mysql.ps1).");

    private MySqlIdempotencySettings Settings(TimeSpan? ttl = null, TimeSpan? lease = null, string? connectionString = null) => new()
    {
        ConnectionString = connectionString ?? ConnectionString!,
        TableName        = _tableName,
        Ttl              = ttl ?? TimeSpan.FromHours(1),
        LeaseTimeout     = lease ?? TimeSpan.FromMinutes(5),
        CleanupBatchSize = 2
    };

    private MySqlIdempotencyStore CreateStore(TimeSpan? ttl = null, TimeSpan? lease = null, string? connectionString = null)
        => new(Settings(ttl, lease, connectionString), NullLogger<MySqlIdempotencyStore>.Instance);

    // --- Settings and registration (always run) ---

    [Fact]
    public void InvalidSettings_ThrowAtRegistration()
    {
        Assert.Throws<ArgumentException>(() => new MySqlIdempotencySettings { ConnectionString = " " }.Validate());
        Assert.Throws<ArgumentException>(() => new MySqlIdempotencySettings { TableName = "db." }.Validate());
        Assert.Throws<ArgumentException>(() => new MySqlIdempotencySettings { Ttl = TimeSpan.FromMilliseconds(500) }.Validate());
        Assert.Throws<ArgumentException>(() => new MySqlIdempotencySettings { LeaseTimeout = TimeSpan.Zero }.Validate());
        Assert.Throws<ArgumentException>(() => new MySqlIdempotencySettings { CleanupInterval = TimeSpan.FromSeconds(-1) }.Validate());
        Assert.Throws<ArgumentException>(() => new MySqlIdempotencySettings { CleanupBatchSize = 0 }.Validate());

        Assert.Throws<ArgumentException>(() => new ServiceCollection().AddNymBroker()
            .AddMySqlIdempotency(new MySqlIdempotencySettings { TableName = "" }));
    }

    [Fact]
    public void Settings_RoundSecondsUp()
    {
        var s = new MySqlIdempotencySettings { Ttl = TimeSpan.FromSeconds(1.2), LeaseTimeout = TimeSpan.FromSeconds(30) };
        Assert.Equal(2, s.TtlSeconds);
        Assert.Equal(30, s.LeaseSeconds);
    }

    [Theory]
    [InlineData("idem", "`idem`")]
    [InlineData("app.idem", "`app`.`idem`")]
    public void Sql_QuotesTableName(string name, string quoted)
        => Assert.Equal(quoted, MySqlIdempotencySql.QuoteTableName(name));

    [Theory]
    [InlineData("my`table")]
    [InlineData("a.b.c")]
    [InlineData("x; DROP TABLE y")]
    public void Sql_RejectsUnsafeTableNames(string name)
        => Assert.Throws<ArgumentException>(() => new MySqlIdempotencySettings { TableName = name }.Validate());

    [Fact]
    public async Task AddMySqlIdempotency_RegistersStoreAndCleanupService_AndBrokerUsesIt()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker().AddMySqlIdempotency(new MySqlIdempotencySettings { TableName = "x" }).Build();
        await using var sp = services.BuildServiceProvider();

        Assert.IsType<MySqlIdempotencyStore>(sp.GetRequiredService<IIdempotencyStore>());
        Assert.Contains(sp.GetServices<IHostedService>(), s => s is IdempotencyCleanupService<MySqlIdempotencyStore>);
    }

    [Fact]
    public async Task AddMySqlIdempotency_ZeroCleanupInterval_RegistersNoCleanupService()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker().AddMySqlIdempotency(new MySqlIdempotencySettings { CleanupInterval = TimeSpan.Zero }).Build();
        await using var sp = services.BuildServiceProvider();

        Assert.DoesNotContain(sp.GetServices<IHostedService>(), s => s is IdempotencyCleanupService<MySqlIdempotencyStore>);
    }

    // --- Store behaviour (MySQL) ---

    [Fact]
    public async Task Claim_InProgress_Complete_Duplicate()
    {
        RequireMySql();
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
        RequireMySql();
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
        RequireMySql();
        var store = CreateStore();
        var id = Guid.NewGuid();

        await store.CompleteAsync(id, Ct);   // e.g. the lease expired and cleanup removed the claim

        Assert.Equal(IdempotencyClaimResult.Duplicate, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task ExpiredLease_CanBeTakenOver()
    {
        RequireMySql();
        var store = CreateStore(lease: TimeSpan.FromSeconds(1));
        var id = Guid.NewGuid();

        await store.TryClaimAsync(id, Ct);
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);

        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task ExpiredTtl_AllowsReprocessing()
    {
        RequireMySql();
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
        RequireMySql();
        var shortLived = CreateStore(ttl: TimeSpan.FromSeconds(1));
        var longLived = CreateStore();
        foreach (var _ in Enumerable.Range(0, 5))
            await shortLived.CompleteAsync(Guid.NewGuid(), Ct);
        var kept = Guid.NewGuid();
        await longLived.CompleteAsync(kept, Ct);
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);

        var deleted = await longLived.DeleteExpiredAsync(Ct);   // CleanupBatchSize = 2 → three statements

        Assert.Equal(5, deleted);
        Assert.Equal(1L, await CountRowsAsync());
        Assert.Equal(IdempotencyClaimResult.Duplicate, await longLived.TryClaimAsync(kept, Ct));
    }

    [Fact]
    public async Task AutoCreate_IsIdempotent_AcrossStores()
    {
        RequireMySql();
        var id = Guid.NewGuid();

        Assert.Equal(IdempotencyClaimResult.Claimed, await CreateStore().TryClaimAsync(id, Ct));
        Assert.Equal(IdempotencyClaimResult.InProgress, await CreateStore().TryClaimAsync(id, Ct));   // second store, same table
    }

    [Fact]
    public async Task ConcurrentClaims_ExactlyOneWins()
    {
        RequireMySql();
        var stores = Enumerable.Range(0, 4).Select(_ => CreateStore()).ToList();
        await stores[0].ReleaseAsync(Guid.NewGuid(), Ct);   // create the table up front
        var id = Guid.NewGuid();

        var results = await Task.WhenAll(Enumerable.Range(0, 16)
            .Select(i => Task.Run(() => stores[i % stores.Count].TryClaimAsync(id, Ct).AsTask(), Ct)));

        Assert.Single(results, r => r == IdempotencyClaimResult.Claimed);
        Assert.All(results.Where(r => r != IdempotencyClaimResult.Claimed), r => Assert.Equal(IdempotencyClaimResult.InProgress, r));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Claim_DoesNotDependOnTheAffectedRowsSetting(bool useAffectedRows)
    {
        // MySqlConnector's UseAffectedRows (CLIENT_FOUND_ROWS) changes what INSERT … ON DUPLICATE KEY UPDATE reports;
        // the store decides by its claim token instead, so both settings must give the same answers.
        RequireMySql();
        var cs = new MySqlConnectionStringBuilder(ConnectionString!) { UseAffectedRows = useAffectedRows }.ConnectionString;
        var store = CreateStore(lease: TimeSpan.FromSeconds(1), connectionString: cs);
        var id = Guid.NewGuid();

        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(id, Ct));
        Assert.Equal(IdempotencyClaimResult.InProgress, await store.TryClaimAsync(id, Ct));
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);
        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(id, Ct));   // expired lease taken over
        await store.CompleteAsync(id, Ct);
        Assert.Equal(IdempotencyClaimResult.Duplicate, await store.TryClaimAsync(id, Ct));
        Assert.Equal(IdempotencyClaimResult.Duplicate, await store.TryClaimAsync(id, Ct));   // unchanged row: still a duplicate
    }

    // --- End to end: two brokers sharing one table ---

    [MessageName("tests.idempotency.mysql.order")]
    public sealed class MySqlSharedOrder { public Guid Marker { get; set; } = Guid.NewGuid(); }

    public sealed class MySqlSlowOrderConsumer : IConsume<MySqlSharedOrder>
    {
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<Guid, int> Calls = new();
        public static int CallsFor(Guid marker) => Calls.GetValueOrDefault(marker);

        public async Task ConsumeAsync(MySqlSharedOrder message, IMessageContext context, CancellationToken ct = default)
        {
            Calls.AddOrUpdate(message.Marker, 1, (_, n) => n + 1);
            await Task.Delay(300, ct);   // keep the claim open while the other broker receives the same message
        }
    }

    [Fact]
    public async Task TwoBrokersOnOneTable_SameMessageDeliveredToBoth_IsConsumedOnce()
    {
        RequireMySql();
        var settings = Settings();

        async Task<(ServiceProvider Provider, NymBrokerImpl Broker)> BuildAsync()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddNymBroker().AddConsumer<MySqlSlowOrderConsumer>().AddMySqlIdempotency(settings).Build();
            var provider = services.BuildServiceProvider();
            var broker = provider.GetRequiredService<NymBrokerImpl>();
            await broker.StartAsync(Ct);
            return (provider, broker);
        }

        var (sp1, broker1) = await BuildAsync();
        var (sp2, broker2) = await BuildAsync();
        try
        {
            var order = new MySqlSharedOrder();
            string json;
            using (var stream = sp1.GetRequiredService<MessageSerializerJson>().Serialize(new MessageContext<MySqlSharedOrder> { Message = order }))
            using (var reader = new StreamReader(stream))
                json = await reader.ReadToEndAsync(Ct);

            var results = await Task.WhenAll(broker1.ProcessAsync(json, "In", Ct), broker2.ProcessAsync(json, "In", Ct));
            var redelivered = await broker2.ProcessAsync(json, "In", Ct);   // e.g. the loser's transport retried

            Assert.Equal(1, MySqlSlowOrderConsumer.CallsFor(order.Marker));
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

    private async Task<long> CountRowsAsync()
    {
        await using var conn = new MySqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = new MySqlCommand($"SELECT COUNT(*) FROM {_tableName}", conn);
        return (long)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString)) return;
        await using var conn = new MySqlConnection(ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new MySqlCommand($"DROP TABLE IF EXISTS {_tableName}", conn);
        await cmd.ExecuteNonQueryAsync();
    }
}
