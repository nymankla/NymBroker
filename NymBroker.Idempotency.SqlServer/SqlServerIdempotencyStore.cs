using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Idempotency;

namespace NymBroker.Idempotency.SqlServer;

/// <summary>
/// Durable <see cref="IIdempotencyStore"/> in a SQL Server table: duplicates are detected across restarts and across
/// every broker instance that uses the same table. One pooled <see cref="SqlConnection"/> per operation (not thread-safe).
/// </summary>
public sealed class SqlServerIdempotencyStore : IIdempotencyStore
{
    private readonly SqlServerIdempotencySettings _settings;
    private readonly ILogger<SqlServerIdempotencyStore> _logger;
    private readonly string _claimSql;
    private readonly string _completeSql;
    private readonly string _releaseSql;
    private readonly string _deleteExpiredSql;
    private readonly SemaphoreSlim _schemaLock = new(1, 1);
    private volatile bool _schemaReady;

    public SqlServerIdempotencyStore(SqlServerIdempotencySettings settings, ILogger<SqlServerIdempotencyStore> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();
        _settings = settings;
        _logger = logger;
        _claimSql = SqlServerIdempotencySql.Claim(settings.TableName);
        _completeSql = SqlServerIdempotencySql.Complete(settings.TableName);
        _releaseSql = SqlServerIdempotencySql.Release(settings.TableName);
        _deleteExpiredSql = SqlServerIdempotencySql.DeleteExpired(settings.TableName);
        _schemaReady = !settings.AutoCreateTable;
    }

    public async ValueTask<IdempotencyClaimResult> TryClaimAsync(Guid messageId, CancellationToken ct = default)
    {
        await EnsureSchemaAsync(ct);
        await using var connection = await OpenAsync(ct);
        await using var command = Command(connection, _claimSql, messageId);
        command.Parameters.Add("@leaseSeconds", SqlDbType.Int).Value = _settings.LeaseSeconds;

        return await command.ExecuteScalarAsync(ct) switch
        {
            SqlServerIdempotencySql.ClaimedResult => IdempotencyClaimResult.Claimed,
            SqlServerIdempotencySql.DuplicateResult => IdempotencyClaimResult.Duplicate,
            // InProgressResult, or no row: the conflicting row vanished in between — let the transport retry.
            _ => IdempotencyClaimResult.InProgress
        };
    }

    public async ValueTask CompleteAsync(Guid messageId, CancellationToken ct = default)
    {
        await EnsureSchemaAsync(ct);
        await using var connection = await OpenAsync(ct);
        await using var command = Command(connection, _completeSql, messageId);
        command.Parameters.Add("@ttlSeconds", SqlDbType.Int).Value = _settings.TtlSeconds;
        await command.ExecuteNonQueryAsync(ct);
    }

    public async ValueTask ReleaseAsync(Guid messageId, CancellationToken ct = default)
    {
        await EnsureSchemaAsync(ct);
        await using var connection = await OpenAsync(ct);
        await using var command = Command(connection, _releaseSql, messageId);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Deletes all expired rows, in batches of <see cref="SqlServerIdempotencySettings.CleanupBatchSize"/>. Returns the number deleted.</summary>
    public async Task<int> DeleteExpiredAsync(CancellationToken ct = default)
    {
        await EnsureSchemaAsync(ct);
        var total = 0;
        while (true)
        {
            await using var connection = await OpenAsync(ct);
            await using var command = new SqlCommand(_deleteExpiredSql, connection);
            command.Parameters.Add("@batchSize", SqlDbType.Int).Value = _settings.CleanupBatchSize;
            var deleted = Convert.ToInt32(await command.ExecuteScalarAsync(ct));
            total += deleted;
            if (deleted < _settings.CleanupBatchSize)
                return total;
        }
    }

    private async Task EnsureSchemaAsync(CancellationToken ct)
    {
        if (_schemaReady) return;

        await _schemaLock.WaitAsync(ct);
        try
        {
            if (_schemaReady) return;
            await using var connection = await OpenAsync(ct);
            await using var command = new SqlCommand(SqlServerIdempotencySql.CreateSchema(_settings.TableName), connection);
            await command.ExecuteNonQueryAsync(ct);
            _schemaReady = true;
            _logger.LogDebug("Idempotency table {Table} is ready", _settings.TableName);
        }
        finally
        {
            _schemaLock.Release();
        }
    }

    private async Task<SqlConnection> OpenAsync(CancellationToken ct)
    {
        var connection = new SqlConnection(_settings.ConnectionString);
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

    private static SqlCommand Command(SqlConnection connection, string sql, Guid messageId)
    {
        var command = new SqlCommand(sql, connection);
        command.Parameters.Add("@id", SqlDbType.UniqueIdentifier).Value = messageId;
        return command;
    }
}
