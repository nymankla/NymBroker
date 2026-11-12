using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core.DI;
using NymBroker.Core.Factory;
using NymBroker.Core.Idempotency;
using NymBroker.Idempotency.SqlServer;

namespace NymBroker.Tests;

/// <summary>
/// Tests for <see cref="SqlServerIdempotencyStore"/> (#55). Settings and registration tests always run; the contract tests
/// need a real SQL Server and are skipped unless <c>NYMBROKER_SQLSERVER_CS</c> is set (see <see cref="SqlServerEndPointTests"/>).
/// </summary>
[Collection("SQL Server integration")]
public sealed class SqlServerIdempotencyStoreTests : IdempotencyStoreContractTests
{
    private const string ConnectionStringVariable = "NYMBROKER_SQLSERVER_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);

    private string QualifiedTableName => $"dbo.{TableName}";

    protected override void RequireDatabase()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString),
            $"Set {ConnectionStringVariable} to run SQL Server integration tests.");

    private SqlServerIdempotencySettings Settings(TimeSpan? ttl = null, TimeSpan? lease = null) => new()
    {
        ConnectionString = ConnectionString!,
        TableName        = QualifiedTableName,
        Ttl              = ttl ?? TimeSpan.FromHours(1),
        LeaseTimeout     = lease ?? TimeSpan.FromMinutes(5),
        CleanupBatchSize = 2
    };

    protected override IExpiringIdempotencyStore CreateStore(TimeSpan ttl, TimeSpan lease, bool shared)
        => new SqlServerIdempotencyStore(Settings(ttl, lease), NullLogger<SqlServerIdempotencyStore>.Instance);

    protected override NymBrokerBuilder AddStore(NymBrokerBuilder builder) => builder.AddSqlServerIdempotency(Settings());

    protected override async Task<long> CountRowsAsync()
    {
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {QualifiedTableName}";
        return (int)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    protected override async ValueTask DropTableAsync()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString)) return;
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DROP TABLE IF EXISTS {QualifiedTableName}";
        await cmd.ExecuteNonQueryAsync();
    }

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
        Assert.Contains(sp.GetServices<IHostedService>(), s => s is IdempotencyCleanupService<SqlServerIdempotencyStore>);
    }

    [Fact]
    public async Task AddSqlServerIdempotency_ZeroCleanupInterval_RegistersNoCleanupService()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker().AddSqlServerIdempotency(new SqlServerIdempotencySettings { CleanupInterval = TimeSpan.Zero }).Build();
        await using var sp = services.BuildServiceProvider();

        Assert.DoesNotContain(sp.GetServices<IHostedService>(), s => s is IdempotencyCleanupService<SqlServerIdempotencyStore>);
    }

    // --- Store behaviour (SQL Server); the shared behaviour is in IdempotencyStoreContractTests ---

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
