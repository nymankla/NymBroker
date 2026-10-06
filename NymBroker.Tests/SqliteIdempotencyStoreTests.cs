using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Data.Sqlite;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Idempotency;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;
using NymBroker.Idempotency.Sqlite;

namespace NymBroker.Tests;

/// <summary>
/// Tests for the SQLite idempotency store. They always run: single-store tests use <c>:memory:</c>, tests that need several
/// stores or brokers share a temporary database file.
/// </summary>
public sealed class SqliteIdempotencyStoreTests : IAsyncLifetime
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private readonly string _tableName = $"nb_idem_{Guid.NewGuid():N}";
    private readonly string _filePath = Path.Combine(Path.GetTempPath(), $"nb_idem_{Guid.NewGuid():N}.db");
    private readonly List<SqliteIdempotencyStore> _stores = [];

    private string SharedConnectionString => $"Data Source={_filePath};Pooling=False";


    private static SqliteIdempotencySettings ValidSettings() => new()
    {
        ConnectionString = "Data Source=:memory:"
    };

    private SqliteIdempotencySettings Settings(TimeSpan? ttl = null, TimeSpan? lease = null, bool shared = false) => new()
    {
        ConnectionString = shared ? SharedConnectionString : "Data Source=:memory:",
        TableName = _tableName,
        Ttl = ttl ?? TimeSpan.FromHours(1),
        LeaseTimeout = lease ?? TimeSpan.FromMinutes(5),
        CleanupBatchSize = 2
    };

    private SqliteIdempotencyStore CreateStore(TimeSpan? ttl = null, TimeSpan? lease = null, bool shared = false)
    {
        var store = new SqliteIdempotencyStore(Settings(ttl, lease, shared), NullLogger<SqliteIdempotencyStore>.Instance);
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
        invalidTable.TableName = " ";
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
            .AddSqliteIdempotency(registrationSettings));
    }

    [Fact]
    public void Settings_RoundSecondsUp()
    {
        var settings = new SqliteIdempotencySettings { Ttl = TimeSpan.FromSeconds(1.2), LeaseTimeout = TimeSpan.FromSeconds(30) };
        Assert.Equal(2, settings.TtlSeconds);
        Assert.Equal(30, settings.LeaseSeconds);
    }

    [Fact]
    public void Sql_QuotesTableName()
    {
        var sql = SqliteIdempotencySql.Claim("odd schema.my\"table");
        Assert.Contains("\"odd schema.my\"\"table\"", sql);
        Assert.Contains("ON CONFLICT(message_id)", sql);
    }

    [Fact]
    public async Task AddSqliteIdempotency_RegistersStoreAndCleanupService()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker().AddSqliteIdempotency(new SqliteIdempotencySettings
        {
            ConnectionString = "Data Source=:memory:",
            TableName = "x"
        }).Build();
        await using var provider = services.BuildServiceProvider();

        Assert.IsType<SqliteIdempotencyStore>(provider.GetRequiredService<IIdempotencyStore>());
        Assert.Contains(provider.GetServices<IHostedService>(), service => service is SqliteIdempotencyCleanupService);
    }

    [Fact]
    public async Task AddSqliteIdempotency_ZeroCleanupInterval_RegistersNoCleanupService()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker().AddSqliteIdempotency(new SqliteIdempotencySettings
        {
            ConnectionString = "Data Source=:memory:",
            CleanupInterval = TimeSpan.Zero
        }).Build();
        await using var provider = services.BuildServiceProvider();

        Assert.DoesNotContain(provider.GetServices<IHostedService>(), service => service is SqliteIdempotencyCleanupService);
    }

    [Fact]
    public async Task Claim_InProgress_Complete_Duplicate()
    {
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
        var store = CreateStore();
        var id = Guid.NewGuid();

        await store.CompleteAsync(id, Ct);

        Assert.Equal(IdempotencyClaimResult.Duplicate, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task ExpiredLease_CanBeTakenOver()
    {
        var store = CreateStore(lease: TimeSpan.FromSeconds(1));
        var id = Guid.NewGuid();

        await store.TryClaimAsync(id, Ct);
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);

        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task ExpiredTtl_AllowsReprocessing()
    {
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
        var shortLived = CreateStore(ttl: TimeSpan.FromSeconds(1), shared: true);
        var longLived = CreateStore(shared: true);
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
        var id = Guid.NewGuid();

        Assert.Equal(IdempotencyClaimResult.Claimed, await CreateStore(shared: true).TryClaimAsync(id, Ct));
        Assert.Equal(IdempotencyClaimResult.InProgress, await CreateStore(shared: true).TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task ConcurrentClaims_ExactlyOneWins()
    {
        var stores = Enumerable.Range(0, 4).Select(_ => CreateStore(shared: true)).ToList();
        await stores[0].ReleaseAsync(Guid.NewGuid(), Ct);
        var id = Guid.NewGuid();

        var results = await Task.WhenAll(Enumerable.Range(0, 16)
            .Select(i => Task.Run(() => stores[i % stores.Count].TryClaimAsync(id, Ct).AsTask(), Ct)));

        Assert.Single(results, result => result == IdempotencyClaimResult.Claimed);
        Assert.All(results.Where(result => result != IdempotencyClaimResult.Claimed),
            result => Assert.Equal(IdempotencyClaimResult.InProgress, result));
    }

    [MessageName("tests.idempotency.sqlite.order")]
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
        var settings = Settings(shared: true);

        async Task<(ServiceProvider Provider, NymBrokerImpl Broker)> BuildAsync()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddNymBroker().AddConsumer<SlowOrderConsumer>().AddSqliteIdempotency(settings).Build();
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

    private sealed class NativeEndPoint : IEndPointEventDriven
    {
        public EndpointMode Mode => EndpointMode.ReadWrite;
        public bool UsesNativeDeadLetter => true;
        public Task PostAsync(byte[] message, CancellationToken ct = default) => Task.CompletedTask;
        public NymBroker.Core.Endpoint.HealthCheck.IHealthCheckResult HealthCheck() => NymBroker.Core.Endpoint.HealthCheck.HealthCheckResult.Healthy();
        public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct) => Task.CompletedTask;
        public Task StopListeningAsync() => Task.CompletedTask;
    }

    [MessageName("tests.idempotency.sqlite.flaky")]
    public sealed class FlakyOrder { public Guid Marker { get; set; } = Guid.NewGuid(); }

    public sealed class FlakyOrderConsumer : IConsume<FlakyOrder>
    {
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<Guid, int> Calls = new();
        public static int CallsFor(Guid marker) => Calls.GetValueOrDefault(marker);

        public Task ConsumeAsync(FlakyOrder message, IMessageContext context, CancellationToken ct = default)
        {
            var calls = Calls.AddOrUpdate(message.Marker, 1, (_, count) => count + 1);
            if (calls == 1) throw new InvalidOperationException("first attempt fails");
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task Broker_RetryLosesNoMessage_AndRealDuplicateIsDropped()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker().AddConsumer<FlakyOrderConsumer>().AddSqliteIdempotency(Settings()).Build();
        await using var provider = services.BuildServiceProvider();
        var broker = provider.GetRequiredService<NymBrokerImpl>();
        broker.AddEndpoint("Native", new NativeEndPoint());
        await broker.StartAsync(Ct);
        try
        {
            var order = new FlakyOrder();
            string json;
            using (var stream = provider.GetRequiredService<MessageSerializerJson>().Serialize(new MessageContext<FlakyOrder> { Message = order }))
            using (var reader = new StreamReader(stream))
                json = await reader.ReadToEndAsync(Ct);

            Assert.Equal(ProcessOutcome.Retry, (await broker.ProcessAsync(json, "Native", Ct)).Outcome);
            Assert.Equal(ProcessOutcome.Completed, (await broker.ProcessAsync(json, "Native", Ct)).Outcome);
            Assert.Equal(ProcessOutcome.Completed, (await broker.ProcessAsync(json, "Native", Ct)).Outcome);

            Assert.Equal(2, FlakyOrderConsumer.CallsFor(order.Marker));
        }
        finally
        {
            await broker.StopAsync(Ct);
        }
    }

    private async Task<int> CountRowsAsync()
    {
        await using var connection = new SqliteConnection(SharedConnectionString);
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

        SqliteConnection.ClearAllPools();
        foreach (var suffix in new[] { "", "-wal", "-shm" })
            if (File.Exists(_filePath + suffix))
                File.Delete(_filePath + suffix);
    }
}
