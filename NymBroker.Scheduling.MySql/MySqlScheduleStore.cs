using Microsoft.Extensions.Logging;
using MySqlConnector;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Scheduling;

namespace NymBroker.Scheduling.MySql;

/// <summary>
/// Durable <see cref="IScheduleStore"/> in a MySQL 8.0+ / MariaDB 10.6+ table, shared by every broker instance that
/// uses it: each occurrence of a named scheduled action runs once, and missed runs can be caught up after a restart.
/// One pooled <see cref="MySqlConnection"/> per operation (not thread-safe).
/// </summary>
public sealed class MySqlScheduleStore : IScheduleStore
{
    private readonly MySqlScheduleStoreSettings _settings;
    private readonly ILogger<MySqlScheduleStore> _logger;
    private readonly string _table;
    private readonly string _getLastRunSql;
    private readonly string _claimSql;
    private readonly AsyncOnce _schema = new();

    public MySqlScheduleStore(MySqlScheduleStoreSettings settings, ILogger<MySqlScheduleStore> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();
        _settings = settings;
        _logger = logger;
        _table = MySqlScheduleSql.QuoteTableName(settings.TableName);
        _getLastRunSql = MySqlScheduleSql.GetLastRun(_table);
        _claimSql = MySqlScheduleSql.Claim(_table);
    }

    public async ValueTask<DateTimeOffset?> GetLastRunAsync(string name, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(name);
        await EnsureSchemaAsync(ct);
        await using var connection = await OpenAsync(ct);
        await using var command = new MySqlCommand(_getLastRunSql, connection);
        command.Parameters.AddWithValue("@name", name);
        return await command.ExecuteScalarAsync(ct) is DateTime lastRun
            ? new DateTimeOffset(DateTime.SpecifyKind(lastRun, DateTimeKind.Utc))
            : null;
    }

    public async ValueTask<bool> TryClaimAsync(string name, DateTimeOffset occurrence, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(name);
        await EnsureSchemaAsync(ct);
        await using var connection = await OpenAsync(ct);
        await using var command = new MySqlCommand(_claimSql, connection);
        command.Parameters.AddWithValue("@name", name);
        command.Parameters.AddWithValue("@occurrence", occurrence.UtcDateTime);
        command.Parameters.AddWithValue("@token", Guid.NewGuid().ToByteArray());

        await using var reader = await command.ExecuteReaderAsync(ct);
        do
        {
            if (await reader.ReadAsync(ct))
                return reader.GetInt64(0) == 1;
        } while (await reader.NextResultAsync(ct));

        return false;
    }

    private Task EnsureSchemaAsync(CancellationToken ct)
        => _settings.AutoCreateTable ? _schema.RunAsync(this, static (self, ct) => self.CreateSchemaAsync(ct), ct) : Task.CompletedTask;

    private async Task CreateSchemaAsync(CancellationToken ct)
    {
        await using var connection = await OpenAsync(ct);
        await using var command = new MySqlCommand(MySqlScheduleSql.CreateSchema(_table), connection);
        await command.ExecuteNonQueryAsync(ct);
        _logger.LogDebug("Schedule table {Table} is ready", _settings.TableName);
    }

    private async Task<MySqlConnection> OpenAsync(CancellationToken ct)
    {
        var connection = new MySqlConnection(_settings.ConnectionString);
        try
        {
            await connection.OpenAsync(ct);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }
}
