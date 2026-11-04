using System.Globalization;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Scheduling;
using NymBroker.Shared;

namespace NymBroker.Scheduling.Sqlite;

/// <summary>
/// Durable <see cref="IScheduleStore"/> in SQLite: named scheduled actions remember their last run across restarts on
/// one host. One persistent connection, serialized (<see cref="SqliteStoreConnection"/>).
/// </summary>
public sealed class SqliteScheduleStore : IScheduleStore, IAsyncDisposable
{
    private readonly SqliteStoreConnection _db;
    private readonly string _getLastRunSql;
    private readonly string _claimSql;

    public SqliteScheduleStore(SqliteScheduleStoreSettings settings, ILogger<SqliteScheduleStore> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();
        _db = new SqliteStoreConnection(settings.ConnectionString, settings.AutoCreateTable ? SqliteScheduleSql.CreateSchema(settings.TableName) : null,
            logger, $"Schedule table {settings.TableName}");
        _getLastRunSql = SqliteScheduleSql.GetLastRun(settings.TableName);
        _claimSql = SqliteScheduleSql.Claim(settings.TableName);
    }

    public async ValueTask<DateTimeOffset?> GetLastRunAsync(string name, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(name);
        using var db = await _db.AcquireAsync(ct);
        await using var command = db.Connection.CreateCommand();
        command.CommandText = _getLastRunSql;
        command.Parameters.AddWithValue("@name", name);
        return await command.ExecuteScalarAsync(ct) is { } value and not DBNull
            ? DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(value, CultureInfo.InvariantCulture))
            : null;
    }

    public async ValueTask<bool> TryClaimAsync(string name, DateTimeOffset occurrence, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(name);
        using var db = await _db.AcquireAsync(ct);
        await using var command = db.Connection.CreateCommand();
        command.CommandText = _claimSql;
        command.Parameters.AddWithValue("@name", name);
        command.Parameters.AddWithValue("@occurrence", occurrence.ToUnixTimeMilliseconds());
        return await command.ExecuteScalarAsync(ct) is not null;
    }

    public ValueTask DisposeAsync() => _db.DisposeAsync();
}
