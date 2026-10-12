using Microsoft.Extensions.Logging;
using MySqlConnector;
using NymBroker.Core.Idempotency;

namespace NymBroker.Idempotency.MySql;

/// <summary>
/// Durable <see cref="IIdempotencyStore"/> in a MySQL 8.0+ / MariaDB 10.6+ table: duplicates are detected across restarts
/// and across every broker instance that uses the same table. One pooled <see cref="MySqlConnection"/> per operation
/// (not thread-safe).
/// </summary>
public sealed class MySqlIdempotencyStore : IExpiringIdempotencyStore
{
    private readonly MySqlIdempotencySettings _settings;
    private readonly ILogger<MySqlIdempotencyStore> _logger;
    private readonly string _table;
    private readonly string _claimSql;
    private readonly string _completeSql;
    private readonly string _releaseSql;
    private readonly string _deleteExpiredSql;
    private readonly SemaphoreSlim _schemaLock = new(1, 1);
    private volatile bool _schemaReady;

    public MySqlIdempotencyStore(MySqlIdempotencySettings settings, ILogger<MySqlIdempotencyStore> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();
        _settings = settings;
        _logger = logger;
        _table = MySqlIdempotencySql.QuoteTableName(settings.TableName);
        _claimSql = MySqlIdempotencySql.Claim(_table);
        _completeSql = MySqlIdempotencySql.Complete(_table);
        _releaseSql = MySqlIdempotencySql.Release(_table);
        _deleteExpiredSql = MySqlIdempotencySql.DeleteExpired(_table);
        _schemaReady = !settings.AutoCreateTable;
    }

    public async ValueTask<IdempotencyClaimResult> TryClaimAsync(Guid messageId, CancellationToken ct = default)
    {
        await EnsureSchemaAsync(ct);
        await using var connection = await OpenAsync(ct);
        await using var command = Command(connection, _claimSql, messageId);
        command.Parameters.AddWithValue("@token", Guid.NewGuid().ToByteArray());
        command.Parameters.AddWithValue("@leaseSeconds", _settings.LeaseSeconds);

        await using var reader = await command.ExecuteReaderAsync(ct);
        IdempotencyClaimResult result;
        do
        {
            if (await reader.ReadAsync(ct))
            {
                result = reader.GetInt64(1) == 1 ? IdempotencyClaimResult.Claimed
                    : reader.GetInt32(0) == MySqlIdempotencySql.Completed ? IdempotencyClaimResult.Duplicate
                    : IdempotencyClaimResult.InProgress;
                return result;
            }
        } while (await reader.NextResultAsync(ct));

        // The row vanished between the upsert and the read: let the transport retry.
        return IdempotencyClaimResult.InProgress;
    }

    public async ValueTask CompleteAsync(Guid messageId, CancellationToken ct = default)
    {
        await EnsureSchemaAsync(ct);
        await using var connection = await OpenAsync(ct);
        await using var command = Command(connection, _completeSql, messageId);
        command.Parameters.AddWithValue("@ttlSeconds", _settings.TtlSeconds);
        await command.ExecuteNonQueryAsync(ct);
    }

    public async ValueTask ReleaseAsync(Guid messageId, CancellationToken ct = default)
    {
        await EnsureSchemaAsync(ct);
        await using var connection = await OpenAsync(ct);
        await using var command = Command(connection, _releaseSql, messageId);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Deletes all expired rows, in batches of <see cref="MySqlIdempotencySettings.CleanupBatchSize"/>. Returns the number deleted.</summary>
    public async Task<int> DeleteExpiredAsync(CancellationToken ct = default)
    {
        await EnsureSchemaAsync(ct);
        var total = 0;
        while (true)
        {
            await using var connection = await OpenAsync(ct);
            await using var command = new MySqlCommand(_deleteExpiredSql, connection);
            command.Parameters.AddWithValue("@batchSize", _settings.CleanupBatchSize);
            var deleted = await command.ExecuteNonQueryAsync(ct);
            total += deleted;
            if (deleted < _settings.CleanupBatchSize)
                return total;
        }
    }

    private async ValueTask EnsureSchemaAsync(CancellationToken ct)
    {
        if (_schemaReady) return;

        await _schemaLock.WaitAsync(ct);
        try
        {
            if (_schemaReady) return;
            await using var connection = await OpenAsync(ct);
            await using var command = new MySqlCommand(MySqlIdempotencySql.CreateSchema(_table), connection);
            await command.ExecuteNonQueryAsync(ct);
            _schemaReady = true;
            _logger.LogDebug("Idempotency table {Table} is ready", _settings.TableName);
        }
        finally
        {
            _schemaLock.Release();
        }
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

    private static MySqlCommand Command(MySqlConnection connection, string sql, Guid messageId)
    {
        var command = new MySqlCommand(sql, connection);
        command.Parameters.AddWithValue("@id", messageId.ToByteArray());
        return command;
    }
}
