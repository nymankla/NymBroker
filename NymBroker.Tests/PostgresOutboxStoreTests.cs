using System.Data.Common;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NymBroker.Core.Outbox;
using NymBroker.Outbox.Postgres;

namespace NymBroker.Tests;

/// <summary>The outbox contract on PostgreSQL; skipped unless <c>NYMBROKER_POSTGRES_CS</c> is set. Settings tests always run.</summary>
public sealed class PostgresOutboxStoreTests : OutboxStoreContractTests
{
    private const string ConnectionStringVariable = "NYMBROKER_POSTGRES_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);

    protected override void RequireDatabase()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString), $"Set {ConnectionStringVariable} to run PostgreSQL integration tests.");

    protected override IOutboxStore CreateStore()
        => new PostgresOutboxStore(new PostgresOutboxSettings { ConnectionString = ConnectionString!, TableName = TableName },
            NullLogger<PostgresOutboxStore>.Instance);

    protected override async Task<DbConnection> OpenConnectionAsync()
    {
        var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync(Ct);
        return connection;
    }

    protected override async ValueTask DropTableAsync()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString)) return;
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand($"DROP TABLE IF EXISTS \"{TableName}\"", connection);
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public void InvalidSettings_Throw()
    {
        Assert.Throws<ArgumentException>(() => new PostgresOutboxSettings().Validate());
        Assert.Throws<ArgumentException>(() => new PostgresOutboxSettings { ConnectionString = "Host=x", TableName = "public." }.Validate());
    }

    [Fact]
    public void Sql_QuotesTableName_AndSkipsLockedRows()
    {
        var sql = PostgresOutboxSql.Statements("public.my\"table");
        Assert.Contains("\"public\".\"my\"\"table\"", sql.Claim);
        Assert.Contains("SKIP LOCKED", sql.Claim);
    }
}
