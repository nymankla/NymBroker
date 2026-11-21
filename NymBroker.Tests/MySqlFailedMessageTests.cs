using Microsoft.Extensions.Logging.Abstractions;
using MySqlConnector;
using NymBroker.Endpoint.MySql;

namespace NymBroker.Tests;

/// <summary>The failed-message contract (replay and removal) on MySQL; skipped unless <c>NYMBROKER_MYSQL_CS</c> is set.</summary>
public sealed class MySqlFailedMessageTests : FailedMessageContractTests<MySqlEndPoint>
{
    private const string ConnectionStringVariable = "NYMBROKER_MYSQL_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);
    private readonly string _table = $"nb_replay_{Guid.NewGuid():N}";

    protected override void RequireDatabase()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString), $"Set {ConnectionStringVariable} to run MySQL integration tests (scripts/setup-mysql.ps1).");

    protected override MySqlEndPoint CreateEndPoint(int maxRetryCount) => new("replay", new MySqlSettings
    {
        ConnectionString = ConnectionString!,
        TableName = _table,
        MaxRetryCount = maxRetryCount,
        PollInterval = TimeSpan.FromMilliseconds(20)
    }, NullLogger<MySqlEndPoint>.Instance);

    protected override Task DeleteFailedRowsAsync() => ExecuteAsync($"DELETE FROM `{_table}` WHERE status = 3");

    protected override Task SetFailedAtAsync(DateTimeOffset failedAt)
        => ExecuteAsync($"UPDATE `{_table}` SET failed_at_utc = '{failedAt.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture)}' WHERE status = 3");

    protected override async ValueTask DropTableAsync()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString)) return;
        await ExecuteAsync($"DROP TABLE IF EXISTS `{_table}_deferred`; DROP TABLE IF EXISTS `{_table}`;", CancellationToken.None);
    }

    private static async Task ExecuteAsync(string sql, CancellationToken? ct = null)
    {
        await using var connection = new MySqlConnection(ConnectionString);
        await connection.OpenAsync(ct ?? Ct);
        await using var command = new MySqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct ?? Ct);
    }
}
