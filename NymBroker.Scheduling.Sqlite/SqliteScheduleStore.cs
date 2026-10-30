using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Scheduling;

namespace NymBroker.Scheduling.Sqlite;

/// <summary>
/// Durable <see cref="IScheduleStore"/> in SQLite: named scheduled actions remember their last run across restarts on
/// one host. One persistent connection (also required for <c>Data Source=:memory:</c>), serialized by a semaphore.
/// File databases use WAL mode and a busy timeout so several processes on the host can share them.
/// </summary>
public sealed class SqliteScheduleStore : IScheduleStore, IAsyncDisposable
{
    private readonly SqliteScheduleStoreSettings _settings;
    private readonly ILogger<SqliteScheduleStore> _logger;
    private readonly string _getLastRunSql;
    private readonly string _claimSql;
    private readonly SemaphoreSlim _dbLock = new(1, 1);
    private SqliteConnection? _connection;
    private bool _disposed;

    public SqliteScheduleStore(SqliteScheduleStoreSettings settings, ILogger<SqliteScheduleStore> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();
        _settings = settings;
        _logger = logger;
        _getLastRunSql = SqliteScheduleSql.GetLastRun(settings.TableName);
        _claimSql = SqliteScheduleSql.Claim(settings.TableName);
    }

    public async ValueTask<DateTimeOffset?> GetLastRunAsync(string name, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(name);
        await _dbLock.WaitAsync(ct);
        try
        {
            var connection = await EnsureConnectionAsync(ct);
            await using var command = connection.CreateCommand();
            command.CommandText = _getLastRunSql;
            command.Parameters.AddWithValue("@name", name);
            return await command.ExecuteScalarAsync(ct) is { } value and not DBNull
                ? DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(value))
                : null;
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public async ValueTask<bool> TryClaimAsync(string name, DateTimeOffset occurrence, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(name);
        await _dbLock.WaitAsync(ct);
        try
        {
            var connection = await EnsureConnectionAsync(ct);
            await using var command = connection.CreateCommand();
            command.CommandText = _claimSql;
            command.Parameters.AddWithValue("@name", name);
            command.Parameters.AddWithValue("@occurrence", occurrence.ToUnixTimeMilliseconds());
            return await command.ExecuteScalarAsync(ct) is not null;
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _dbLock.WaitAsync();
        try
        {
            if (_disposed) return;
            _disposed = true;
            if (_connection is not null)
                await _connection.DisposeAsync();
            _connection = null;
        }
        finally
        {
            _dbLock.Release();
        }
    }

    // Caller holds _dbLock.
    private async Task<SqliteConnection> EnsureConnectionAsync(CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_connection is not null) return _connection;

        var builder = new SqliteConnectionStringBuilder(_settings.ConnectionString);
        var inMemory = builder.Mode == SqliteOpenMode.Memory
                       || string.IsNullOrEmpty(builder.DataSource)
                       || builder.DataSource.Equals(":memory:", StringComparison.OrdinalIgnoreCase);

        var connection = new SqliteConnection(_settings.ConnectionString);
        try
        {
            await connection.OpenAsync(ct);
            await using (var pragma = connection.CreateCommand())
            {
                pragma.CommandText = inMemory
                    ? "PRAGMA busy_timeout=5000;"
                    : "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;";
                await pragma.ExecuteNonQueryAsync(ct);
            }

            if (_settings.AutoCreateTable)
            {
                await using var create = connection.CreateCommand();
                create.CommandText = SqliteScheduleSql.CreateSchema(_settings.TableName);
                await create.ExecuteNonQueryAsync(ct);
                _logger.LogDebug("Schedule table {Table} is ready", _settings.TableName);
            }
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }

        _connection = connection;
        return connection;
    }
}
