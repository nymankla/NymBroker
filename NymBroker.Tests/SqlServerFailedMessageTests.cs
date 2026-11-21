using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Endpoint.SqlServer;

namespace NymBroker.Tests;

/// <summary>The failed-message contract (replay and removal) on SQL Server; skipped unless <c>NYMBROKER_SQLSERVER_CS</c> is set.</summary>
[Collection("SQL Server integration")]
public sealed class SqlServerFailedMessageTests : FailedMessageContractTests<SqlServerEndPoint>
{
    private const string ConnectionStringVariable = "NYMBROKER_SQLSERVER_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);
    private readonly string _table = $"dbo.nb_replay_{Guid.NewGuid():N}";

    protected override void RequireDatabase()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString), $"Set {ConnectionStringVariable} to run SQL Server integration tests.");

    protected override SqlServerEndPoint CreateEndPoint(int maxRetryCount) => new("replay", new SqlServerSettings
    {
        ConnectionString = ConnectionString!,
        TableName = _table,
        MaxRetryCount = maxRetryCount,
        PollInterval = TimeSpan.FromMilliseconds(20)
    }, NullLogger<SqlServerEndPoint>.Instance);

    protected override Task DeleteFailedRowsAsync() => ExecuteAsync($"DELETE FROM {_table} WHERE status = 3");

    protected override Task SetFailedAtAsync(DateTimeOffset failedAt)
        => ExecuteAsync($"UPDATE {_table} SET failed_at_utc = '{failedAt.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.fffffff", System.Globalization.CultureInfo.InvariantCulture)}' WHERE status = 3");

    protected override async ValueTask DropTableAsync()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString)) return;
        await ExecuteAsync($"DROP TABLE IF EXISTS {_table}_deferred; DROP TABLE IF EXISTS {_table};", CancellationToken.None);
    }

    private static async Task ExecuteAsync(string sql, CancellationToken? ct = null)
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync(ct ?? Ct);
        await using var command = new SqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct ?? Ct);
    }
}
