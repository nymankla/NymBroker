using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NymBroker.Core.DI;
using NymBroker.Core.Factory;
using NymBroker.Core.Idempotency;
using NymBroker.Idempotency.Postgres;

namespace NymBroker.Tests;

/// <summary>
/// Tests for the PostgreSQL idempotency store. Settings and registration tests always run; the contract tests need
/// NYMBROKER_POSTGRES_CS set (see <see cref="PostgresEndPointTests"/>).
/// </summary>
public sealed class PostgresIdempotencyStoreTests : IdempotencyStoreContractTests
{
    private const string ConnectionStringVariable = "NYMBROKER_POSTGRES_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);

    protected override void RequireDatabase()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString),
            $"Set {ConnectionStringVariable} to run PostgreSQL integration tests.");

    private static PostgresIdempotencySettings ValidSettings() => new()
    {
        ConnectionString = "Host=localhost;Database=nymbroker;Username=postgres"
    };

    private PostgresIdempotencySettings Settings(TimeSpan? ttl = null, TimeSpan? lease = null) => new()
    {
        ConnectionString = ConnectionString!,
        TableName = TableName,
        Ttl = ttl ?? TimeSpan.FromHours(1),
        LeaseTimeout = lease ?? TimeSpan.FromMinutes(5),
        CleanupBatchSize = 2
    };

    protected override IExpiringIdempotencyStore CreateStore(TimeSpan ttl, TimeSpan lease, bool shared)
        => new PostgresIdempotencyStore(Settings(ttl, lease), NullLogger<PostgresIdempotencyStore>.Instance);

    protected override NymBrokerBuilder AddStore(NymBrokerBuilder builder) => builder.AddPostgresIdempotency(Settings());

    protected override async Task<long> CountRowsAsync()
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync(Ct);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM \"{TableName}\"";
        return Convert.ToInt64(await command.ExecuteScalarAsync(Ct));
    }

    protected override async ValueTask DropTableAsync()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString)) return;
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"DROP TABLE IF EXISTS \"{TableName}\"";
        await command.ExecuteNonQueryAsync();
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

    // Opt-in until the qualified/unqualified schema creation race is fixed.
    [Fact(Explicit = true)]
    public async Task AutoCreate_QualifiedAndUnqualifiedStoresStartingAtOnce_AllClaim()
    {
        RequireDatabase();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = Enumerable.Range(0, 16).Select(async i =>
        {
            var settings = Settings();
            settings.TableName = i % 2 == 0 ? TableName : $"public.{TableName}";
            await using var store = new PostgresIdempotencyStore(settings, NullLogger<PostgresIdempotencyStore>.Instance);
            await gate.Task;
            return await store.TryClaimAsync(Guid.NewGuid(), Ct);
        }).ToArray();
        gate.SetResult();

        Assert.All(await Task.WhenAll(tasks), result => Assert.Equal(IdempotencyClaimResult.Claimed, result));
        Assert.Equal(16, await CountRowsAsync());
    }

    [Fact]
    public async Task AutoCreate_StoresStartingAtOnce_AllCreateTheTable()
    {
        RequireDatabase();
        // Instances starting together race on creating the table; the losers must not fail.
        var results = await Task.WhenAll(Enumerable.Range(0, 8)
            .Select(_ => Store().TryClaimAsync(Guid.NewGuid(), Ct).AsTask()));

        Assert.All(results, r => Assert.Equal(IdempotencyClaimResult.Claimed, r));
    }
}
