using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NymBroker.Endpoint.Postgres;

namespace NymBroker.Tests;

/// <summary>The failed-message contract (replay and removal) on PostgreSQL; skipped unless <c>NYMBROKER_POSTGRES_CS</c> is set.</summary>
public sealed class PostgresFailedMessageTests : FailedMessageContractTests<PostgresEndPoint>
{
    private const string ConnectionStringVariable = "NYMBROKER_POSTGRES_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);
    private readonly string _table = $"nb_replay_{Guid.NewGuid():N}";

    protected override void RequireDatabase()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString), $"Set {ConnectionStringVariable} to run PostgreSQL integration tests.");

    protected override PostgresEndPoint CreateEndPoint(int maxRetryCount) => new("replay", new PostgresSettings
    {
        ConnectionString = ConnectionString!,
        TableName = _table,
        MaxRetryCount = maxRetryCount,
        PollInterval = TimeSpan.FromMilliseconds(20)
    }, NullLogger<PostgresEndPoint>.Instance);

    protected override Task DeleteFailedRowsAsync() => ExecuteAsync($"DELETE FROM \"{_table}\" WHERE status = 3");

    protected override Task SetFailedAtAsync(DateTimeOffset failedAt)
        => ExecuteAsync($"UPDATE \"{_table}\" SET failed_at_utc = '{failedAt.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture)}+00' WHERE status = 3");

    protected override async ValueTask DropTableAsync()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString)) return;
        await ExecuteAsync($"DROP TABLE IF EXISTS \"{_table}_deferred\"; DROP TABLE IF EXISTS \"{_table}\";", CancellationToken.None);
    }

    private static async Task ExecuteAsync(string sql, CancellationToken? ct = null)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync(ct ?? Ct);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct ?? Ct);
    }
}
