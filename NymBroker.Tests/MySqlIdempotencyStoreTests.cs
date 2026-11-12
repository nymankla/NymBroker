using MySqlConnector;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core.DI;
using NymBroker.Core.Factory;
using NymBroker.Core.Idempotency;
using NymBroker.Idempotency.MySql;

namespace NymBroker.Tests;

/// <summary>
/// Tests for <see cref="MySqlIdempotencyStore"/> (#76). Settings and registration tests always run; the rest need
/// a real MySQL and are skipped unless <c>NYMBROKER_MYSQL_CS</c> is set (see <see cref="MySqlEndPointTests"/>).
/// </summary>
public sealed class MySqlIdempotencyStoreTests : IdempotencyStoreContractTests
{
    private const string ConnectionStringVariable = "NYMBROKER_MYSQL_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);

    protected override void RequireDatabase()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString),
            $"Set {ConnectionStringVariable} to run MySQL integration tests (scripts/setup-mysql.ps1).");

    private MySqlIdempotencySettings Settings(TimeSpan? ttl = null, TimeSpan? lease = null, string? connectionString = null) => new()
    {
        ConnectionString = connectionString ?? ConnectionString!,
        TableName        = TableName,
        Ttl              = ttl ?? TimeSpan.FromHours(1),
        LeaseTimeout     = lease ?? TimeSpan.FromMinutes(5),
        CleanupBatchSize = 2
    };

    protected override IExpiringIdempotencyStore CreateStore(TimeSpan ttl, TimeSpan lease, bool shared)
        => new MySqlIdempotencyStore(Settings(ttl, lease), NullLogger<MySqlIdempotencyStore>.Instance);

    protected override NymBrokerBuilder AddStore(NymBrokerBuilder builder) => builder.AddMySqlIdempotency(Settings());

    protected override async Task<long> CountRowsAsync()
    {
        await using var conn = new MySqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = new MySqlCommand($"SELECT COUNT(*) FROM {TableName}", conn);
        return (long)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    protected override async Task<System.Data.Common.DbConnection> OpenSharedConnectionAsync()
    {
        var connection = new MySqlConnection(ConnectionString);
        await connection.OpenAsync(Ct);
        return connection;
    }

    protected override async ValueTask DropTableAsync()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString)) return;
        await using var conn = new MySqlConnection(ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new MySqlCommand($"DROP TABLE IF EXISTS {TableName}", conn);
        await cmd.ExecuteNonQueryAsync();
    }

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

    // --- Store behaviour (MySQL); the shared behaviour is in IdempotencyStoreContractTests ---

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Claim_DoesNotDependOnTheAffectedRowsSetting(bool useAffectedRows)
    {
        // MySqlConnector's UseAffectedRows (CLIENT_FOUND_ROWS) changes what INSERT … ON DUPLICATE KEY UPDATE reports;
        // the store decides by its claim token instead, so both settings must give the same answers.
        RequireDatabase();
        var cs = new MySqlConnectionStringBuilder(ConnectionString!) { UseAffectedRows = useAffectedRows }.ConnectionString;
        var store = new MySqlIdempotencyStore(Settings(lease: TimeSpan.FromSeconds(1), connectionString: cs), NullLogger<MySqlIdempotencyStore>.Instance);
        var id = Guid.NewGuid();

        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(id, Ct));
        Assert.Equal(IdempotencyClaimResult.InProgress, await store.TryClaimAsync(id, Ct));
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);
        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(id, Ct));   // expired lease taken over
        await store.CompleteAsync(id, Ct);
        Assert.Equal(IdempotencyClaimResult.Duplicate, await store.TryClaimAsync(id, Ct));
        Assert.Equal(IdempotencyClaimResult.Duplicate, await store.TryClaimAsync(id, Ct));   // unchanged row: still a duplicate
    }
}
