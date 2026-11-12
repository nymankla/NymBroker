using System.Data.Common;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core.DI;
using NymBroker.Core.Outbox;
using NymBroker.Outbox.SqlServer;

namespace NymBroker.Tests;

/// <summary>The outbox contract on SQL Server; skipped unless <c>NYMBROKER_SQLSERVER_CS</c> is set. Settings tests always run.</summary>
[Collection("SQL Server integration")]
public sealed class SqlServerOutboxStoreTests : OutboxStoreContractTests
{
    private const string ConnectionStringVariable = "NYMBROKER_SQLSERVER_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);

    private string QualifiedTableName => $"dbo.{TableName}";

    protected override void RequireDatabase()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString), $"Set {ConnectionStringVariable} to run SQL Server integration tests.");

    protected override IOutboxStore CreateStore()
        => new SqlServerOutboxStore(new SqlServerOutboxSettings { ConnectionString = ConnectionString!, TableName = QualifiedTableName },
            NullLogger<SqlServerOutboxStore>.Instance);

    protected override async Task<DbConnection> OpenConnectionAsync()
    {
        var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync(Ct);
        return connection;
    }

    protected override async ValueTask DropTableAsync()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString)) return;
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand($"DROP TABLE IF EXISTS {QualifiedTableName}", connection);
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public void InvalidSettings_Throw()
    {
        Assert.Throws<ArgumentException>(() => new SqlServerOutboxSettings { ConnectionString = " " }.Validate());
        Assert.Throws<ArgumentException>(() => new SqlServerOutboxSettings { TableName = "dbo." }.Validate());
        Assert.Throws<ArgumentException>(() => new ServiceCollection().AddNymBroker().AddSqlServerOutbox(new SqlServerOutboxSettings { TableName = "" }));
    }

    [Fact]
    public void Sql_QuotesTableName_AndSkipsLockedRows()
    {
        var sql = SqlServerOutboxSql.Statements("dbo.my]table");
        Assert.Contains("[dbo].[my]]table]", sql.Claim);
        Assert.Contains("READPAST", sql.Claim);
        Assert.Contains("@Resource = N'NymBroker:OutboxSchema'", SqlServerOutboxSql.CreateSchema("dbo.x"));
    }
}
