using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Idempotency;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;
using NymBroker.Idempotency.Postgres;

namespace NymBroker.Tests;

/// <summary>
/// Tests for the PostgreSQL idempotency store. Settings and registration tests always run; database tests need
/// NYMBROKER_POSTGRES_CS set (see <see cref="PostgresEndPointTests"/>).
/// </summary>
public sealed class PostgresIdempotencyStoreTests : IAsyncLifetime
{
    private const string ConnectionStringVariable = "NYMBROKER_POSTGRES_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private readonly string _tableName = $"nb_idem_{Guid.NewGuid():N}";
    private readonly List<PostgresIdempotencyStore> _stores = [];

    private static void RequirePostgres()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString),
            $"Set {ConnectionStringVariable} to run PostgreSQL integration tests.");

    private static PostgresIdempotencySettings ValidSettings() => new()
    {
        ConnectionString = "Host=localhost;Database=nymbroker;Username=postgres"
    };

    private PostgresIdempotencySettings Settings(TimeSpan? ttl = null, TimeSpan? lease = null) => new()
    {
        ConnectionString = ConnectionString!,
        TableName = _tableName,
        Ttl = ttl ?? TimeSpan.FromHours(1),
        LeaseTimeout = lease ?? TimeSpan.FromMinutes(5),
        CleanupBatchSize = 2
    };

    private PostgresIdempotencyStore CreateStore(TimeSpan? ttl = null, TimeSpan? lease = null)
    {
        var store = new PostgresIdempotencyStore(Settings(ttl, lease), NullLogger<PostgresIdempotencyStore>.Instance);
        _stores.Add(store);
        return store;
    }

    [Fact]
    public void InvalidSettings_ThrowAtRegistration()
    {
        var missingConnection = ValidSettings();
        missingConnection.ConnectionString = " ";
        Assert.Throws<ArgumentException>(missingConnection.Validate);

        var invalidTable = ValidSettings();
        invalidTable.TableName = "public.";
        Assert.Throws<ArgumentException>(invalidTable.Validate);

        var invalidTtl = ValidSettings();
        invalidTtl.Ttl = TimeSpan.FromMilliseconds(500);
        Assert.Throws<ArgumentException>(invalidTtl.Validate);

        var invalidLease = ValidSettings();
        invalidLease.LeaseTimeout = TimeSpan.Zero;
        Assert.Throws<ArgumentException>(invalidLease.Validate);

        var invalidCleanupInterval = ValidSettings();
        invalidCleanupInterval.CleanupInterval = TimeSpan.FromSeconds(-1);
        Assert.Throws<ArgumentException>(invalidCleanupInterval.Validate);

        var invalidCleanupBatchSize = ValidSettings();
        invalidCleanupBatchSize.CleanupBatchSize = 0;
        Assert.Throws<ArgumentException>(invalidCleanupBatchSize.Validate);

        var registrationSettings = ValidSettings();
        registrationSettings.TableName = "";
        Assert.Throws<ArgumentException>(() => new ServiceCollection().AddNymBroker()
            .AddPostgresIdempotency(registrationSettings));
    }

    [Fact]
    public void Settings_RoundSecondsUp()
    {
        var settings = new PostgresIdempotencySettings { Ttl = TimeSpan.FromSeconds(1.2), LeaseTimeout = TimeSpan.FromSeconds(30) };
        Assert.Equal(2, settings.TtlSeconds);
        Assert.Equal(30, settings.LeaseSeconds);
    }

    [Fact]
    public void Sql_QuotesTableName()
    {
        var sql = PostgresIdempotencySql.Claim("odd schema.my\"table");
        Assert.Contains("\"odd schema\".\"my\"\"table\"", sql);
        Assert.Contains("ON CONFLICT (message_id)", sql);
    }

    [Fact]
    public async Task AddPostgresIdempotency_RegistersStoreAndCleanupService()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker().AddPostgresIdempotency(new PostgresIdempotencySettings
        {
            ConnectionString = "Host=localhost;Database=nymbroker;Username=postgres",
            TableName = "public.x"
        }).Build();
        await using var provider = services.BuildServiceProvider();

        Assert.IsType<PostgresIdempotencyStore>(provider.GetRequiredService<IIdempotencyStore>());
        Assert.Contains(provider.GetServices<IHostedService>(), service => service is IdempotencyCleanupService<PostgresIdempotencyStore>);
    }

    [Fact]
    public async Task AddPostgresIdempotency_ZeroCleanupInterval_RegistersNoCleanupService()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker().AddPostgresIdempotency(new PostgresIdempotencySettings
        {
            ConnectionString = "Host=localhost;Database=nymbroker;Username=postgres",
            CleanupInterval = TimeSpan.Zero
        }).Build();
        await using var provider = services.BuildServiceProvider();

        Assert.DoesNotContain(provider.GetServices<IHostedService>(), service => service is IdempotencyCleanupService<PostgresIdempotencyStore>);
    }

    [Fact]
    public async Task Claim_InProgress_Complete_Duplicate()
    {
        RequirePostgres();
        var store = CreateStore();
        var id = Guid.NewGuid();

        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(id, Ct));
        Assert.Equal(IdempotencyClaimResult.InProgress, await store.TryClaimAsync(id, Ct));
        await store.CompleteAsync(id, Ct);
        Assert.Equal(IdempotencyClaimResult.Duplicate, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task Release_AllowsClaimAgain_ButDoesNotForgetCompletedMessage()
    {
        RequirePostgres();
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
        RequirePostgres();
        var store = CreateStore();
        var id = Guid.NewGuid();

        await store.CompleteAsync(id, Ct);

        Assert.Equal(IdempotencyClaimResult.Duplicate, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task ExpiredLease_CanBeTakenOver()
    {
        RequirePostgres();
        var store = CreateStore(lease: TimeSpan.FromSeconds(1));
        var id = Guid.NewGuid();

        await store.TryClaimAsync(id, Ct);
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);

        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task ExpiredTtl_AllowsReprocessing()
    {
        RequirePostgres();
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
        RequirePostgres();
        var shortLived = CreateStore(ttl: TimeSpan.FromSeconds(1));
        var longLived = CreateStore();
        foreach (var _ in Enumerable.Range(0, 5))
            await shortLived.CompleteAsync(Guid.NewGuid(), Ct);
        var kept = Guid.NewGuid();
        await longLived.CompleteAsync(kept, Ct);
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);

        Assert.Equal(5, await longLived.DeleteExpiredAsync(Ct));
        Assert.Equal(1, await CountRowsAsync());
        Assert.Equal(IdempotencyClaimResult.Duplicate, await longLived.TryClaimAsync(kept, Ct));
    }

    [Fact]
    public async Task AutoCreate_IsIdempotent_AcrossStores()
    {
        RequirePostgres();
        var id = Guid.NewGuid();

        Assert.Equal(IdempotencyClaimResult.Claimed, await CreateStore().TryClaimAsync(id, Ct));
        Assert.Equal(IdempotencyClaimResult.InProgress, await CreateStore().TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task ConcurrentClaims_ExactlyOneWins()
    {
        RequirePostgres();
        var stores = Enumerable.Range(0, 4).Select(_ => CreateStore()).ToList();
        await stores[0].ReleaseAsync(Guid.NewGuid(), Ct);
        var id = Guid.NewGuid();

        var results = await Task.WhenAll(Enumerable.Range(0, 16)
            .Select(i => Task.Run(() => stores[i % stores.Count].TryClaimAsync(id, Ct).AsTask(), Ct)));

        Assert.Single(results, result => result == IdempotencyClaimResult.Claimed);
        Assert.All(results.Where(result => result != IdempotencyClaimResult.Claimed),
            result => Assert.Equal(IdempotencyClaimResult.InProgress, result));
    }

    [MessageName("tests.idempotency.postgres.order")]
    public sealed class SharedOrder { public Guid Marker { get; set; } = Guid.NewGuid(); }

    public sealed class SlowOrderConsumer : IConsume<SharedOrder>
    {
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<Guid, int> Calls = new();
        public static int CallsFor(Guid marker) => Calls.GetValueOrDefault(marker);

        public async Task ConsumeAsync(SharedOrder message, IMessageContext context, CancellationToken ct = default)
        {
            Calls.AddOrUpdate(message.Marker, 1, (_, count) => count + 1);
            await Task.Delay(300, ct);
        }
    }

    [Fact]
    public async Task TwoBrokersOnOneTable_SameMessageDeliveredToBoth_IsConsumedOnce()
    {
        RequirePostgres();
        var settings = Settings();

        async Task<(ServiceProvider Provider, NymBrokerImpl Broker)> BuildAsync()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddNymBroker().AddConsumer<SlowOrderConsumer>().AddPostgresIdempotency(settings).Build();
            var provider = services.BuildServiceProvider();
            var broker = provider.GetRequiredService<NymBrokerImpl>();
            await broker.StartAsync(Ct);
            return (provider, broker);
        }

        var (provider1, broker1) = await BuildAsync();
        var (provider2, broker2) = await BuildAsync();
        try
        {
            var order = new SharedOrder();
            string json;
            using (var stream = provider1.GetRequiredService<MessageSerializerJson>().Serialize(new MessageContext<SharedOrder> { Message = order }))
            using (var reader = new StreamReader(stream))
                json = await reader.ReadToEndAsync(Ct);

            var results = await Task.WhenAll(broker1.ProcessAsync(json, "In", Ct), broker2.ProcessAsync(json, "In", Ct));
            var redelivered = await broker2.ProcessAsync(json, "In", Ct);

            Assert.Equal(1, SlowOrderConsumer.CallsFor(order.Marker));
            Assert.Contains(results, result => result.Outcome == ProcessOutcome.Completed);
            Assert.Equal(ProcessOutcome.Completed, redelivered.Outcome);
        }
        finally
        {
            await broker1.StopAsync(Ct);
            await broker2.StopAsync(Ct);
            await provider1.DisposeAsync();
            await provider2.DisposeAsync();
        }
    }

    private async Task<int> CountRowsAsync()
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync(Ct);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM \"{_tableName}\"";
        return Convert.ToInt32(await command.ExecuteScalarAsync(Ct));
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var store in _stores)
            await store.DisposeAsync();

        if (string.IsNullOrWhiteSpace(ConnectionString)) return;
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"DROP TABLE IF EXISTS \"{_tableName}\"";
        await command.ExecuteNonQueryAsync();
    }
}
