using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Idempotency;

namespace NymBroker.Idempotency.Sqlite;

/// <summary>
/// Durable <see cref="IIdempotencyStore"/> in SQLite: restart-safe deduplication on one host.
/// One persistent connection (also required for <c>Data Source=:memory:</c>), serialized by a semaphore as in
/// <c>SqliteEndPoint</c>. File databases use WAL mode and a busy timeout so several processes on the host can share them.
/// </summary>
public sealed class SqliteIdempotencyStore : IIdempotencyStore, IAsyncDisposable
{
    private readonly SqliteIdempotencySettings _settings;
    private readonly ILogger<SqliteIdempotencyStore> _logger;
    private readonly string _claimSql;
    private readonly string _existingStatusSql;
    private readonly string _completeSql;
    private readonly string _releaseSql;
    private readonly string _deleteExpiredSql;
    private readonly SemaphoreSlim _dbLock = new(1, 1);
    private SqliteConnection? _connection;
    private bool _disposed;

    public SqliteIdempotencyStore(SqliteIdempotencySettings settings, ILogger<SqliteIdempotencyStore> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();
        _settings = settings;
        _logger = logger;
        _claimSql = SqliteIdempotencySql.Claim(settings.TableName);
        _existingStatusSql = SqliteIdempotencySql.ExistingStatus(settings.TableName);
        _completeSql = SqliteIdempotencySql.Complete(settings.TableName);
        _releaseSql = SqliteIdempotencySql.Release(settings.TableName);
        _deleteExpiredSql = SqliteIdempotencySql.DeleteExpired(settings.TableName);
    }

    public async ValueTask<IdempotencyClaimResult> TryClaimAsync(Guid messageId, CancellationToken ct = default)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            var connection = await EnsureConnectionAsync(ct);
            await using var command = CreateCommand(connection, _claimSql, messageId);
            command.Parameters.AddWithValue("@leaseSeconds", _settings.LeaseSeconds);
            if (await command.ExecuteScalarAsync(ct) is not null)
                return IdempotencyClaimResult.Claimed;

            await using var statusCommand = CreateCommand(connection, _existingStatusSql, messageId);
            return Convert.ToInt64(await statusCommand.ExecuteScalarAsync(ct)) == SqliteIdempotencySql.Completed
                ? IdempotencyClaimResult.Duplicate
                : IdempotencyClaimResult.InProgress;
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public async ValueTask CompleteAsync(Guid messageId, CancellationToken ct = default)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            var connection = await EnsureConnectionAsync(ct);
            await using var command = CreateCommand(connection, _completeSql, messageId);
            command.Parameters.AddWithValue("@ttlSeconds", _settings.TtlSeconds);
            await command.ExecuteNonQueryAsync(ct);
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public async ValueTask ReleaseAsync(Guid messageId, CancellationToken ct = default)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            var connection = await EnsureConnectionAsync(ct);
            await using var command = CreateCommand(connection, _releaseSql, messageId);
            await command.ExecuteNonQueryAsync(ct);
        }
        finally
        {
            _dbLock.Release();
        }
    }

    /// <summary>Deletes expired rows in batches of <see cref="SqliteIdempotencySettings.CleanupBatchSize"/>. Returns the number deleted.</summary>
    public async Task<int> DeleteExpiredAsync(CancellationToken ct = default)
    {
        var total = 0;
        while (true)
        {
            int deleted;
            await _dbLock.WaitAsync(ct);
            try
            {
                var connection = await EnsureConnectionAsync(ct);
                await using var command = connection.CreateCommand();
                command.CommandText = _deleteExpiredSql;
                command.Parameters.AddWithValue("@batchSize", _settings.CleanupBatchSize);
                deleted = await command.ExecuteNonQueryAsync(ct);
            }
            finally
            {
                _dbLock.Release();
            }

            total += deleted;
            if (deleted < _settings.CleanupBatchSize)
                return total;
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

    private static SqliteCommand CreateCommand(SqliteConnection connection, string sql, Guid messageId)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("@id", messageId.ToString("D"));
        return command;
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
                create.CommandText = SqliteIdempotencySql.CreateSchema(_settings.TableName);
                await create.ExecuteNonQueryAsync(ct);
                _logger.LogDebug("Idempotency table {Table} is ready", _settings.TableName);
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
