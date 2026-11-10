using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Data.Sqlite;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Factory;
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
public sealed class SqliteIdempotencyStoreTests : IdempotencyStoreContractTests
{
    private readonly string _filePath = Path.Combine(Path.GetTempPath(), $"nb_idem_{Guid.NewGuid():N}.db");

    private string SharedConnectionString => $"Data Source={_filePath};Pooling=False";

    protected override void RequireDatabase() { }

    private static SqliteIdempotencySettings ValidSettings() => new()
    {
        ConnectionString = "Data Source=:memory:"
    };

    private SqliteIdempotencySettings Settings(TimeSpan? ttl = null, TimeSpan? lease = null, bool shared = false) => new()
    {
        ConnectionString = shared ? SharedConnectionString : "Data Source=:memory:",
        TableName = TableName,
        Ttl = ttl ?? TimeSpan.FromHours(1),
        LeaseTimeout = lease ?? TimeSpan.FromMinutes(5),
        CleanupBatchSize = 2
    };

    protected override IExpiringIdempotencyStore CreateStore(TimeSpan ttl, TimeSpan lease, bool shared)
        => new SqliteIdempotencyStore(Settings(ttl, lease, shared), NullLogger<SqliteIdempotencyStore>.Instance);

    protected override NymBrokerBuilder AddStore(NymBrokerBuilder builder) => builder.AddSqliteIdempotency(Settings(shared: true));

    protected override async Task<long> CountRowsAsync()
    {
        await using var connection = new SqliteConnection(SharedConnectionString);
        await connection.OpenAsync(Ct);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM \"{TableName}\"";
        return Convert.ToInt64(await command.ExecuteScalarAsync(Ct));
    }

    protected override ValueTask DropTableAsync()
    {
        SqliteConnection.ClearAllPools();
        foreach (var suffix in new[] { "", "-wal", "-shm" })
            if (File.Exists(_filePath + suffix))
                File.Delete(_filePath + suffix);
        return ValueTask.CompletedTask;
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
        Assert.Contains(provider.GetServices<IHostedService>(), service => service is IdempotencyCleanupService<SqliteIdempotencyStore>);
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

        Assert.DoesNotContain(provider.GetServices<IHostedService>(), service => service is IdempotencyCleanupService<SqliteIdempotencyStore>);
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
}
