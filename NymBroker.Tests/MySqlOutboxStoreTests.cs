using System.Data.Common;
using Microsoft.Extensions.Logging.Abstractions;
using MySqlConnector;
using NymBroker.Core.Outbox;
using NymBroker.Outbox.MySql;

namespace NymBroker.Tests;

/// <summary>The outbox contract on MySQL; skipped unless <c>NYMBROKER_MYSQL_CS</c> is set. Settings tests always run.</summary>
public sealed class MySqlOutboxStoreTests : OutboxStoreContractTests
{
    private const string ConnectionStringVariable = "NYMBROKER_MYSQL_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);

    protected override void RequireDatabase()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString), $"Set {ConnectionStringVariable} to run MySQL integration tests (scripts/setup-mysql.ps1).");

    protected override IOutboxStore CreateStore()
        => new MySqlOutboxStore(new MySqlOutboxSettings { ConnectionString = ConnectionString!, TableName = TableName },
            NullLogger<MySqlOutboxStore>.Instance);

    protected override async Task<DbConnection> OpenConnectionAsync()
    {
        var connection = new MySqlConnection(ConnectionString);
        await connection.OpenAsync(Ct);
        return connection;
    }

    protected override async ValueTask DropTableAsync()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString)) return;
        await using var connection = new MySqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new MySqlCommand($"DROP TABLE IF EXISTS {TableName}", connection);
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public void InvalidSettings_Throw()
    {
        Assert.Throws<ArgumentException>(() => new MySqlOutboxSettings { ConnectionString = " " }.Validate());
        Assert.Throws<ArgumentException>(() => new MySqlOutboxSettings { TableName = "db." }.Validate());
    }

    [Fact]
    public void Sql_ClaimsInTwoRoundTrips_SkippingLockedRows()
    {
        var sql = MySqlOutboxSql.Statements("`t`");
        Assert.Contains("FOR UPDATE SKIP LOCKED", sql.Claim);
        Assert.Contains("WHERE id IN (1,2)", MySqlOutboxSql.Claim("`t`", [1, 2]));
    }
}
