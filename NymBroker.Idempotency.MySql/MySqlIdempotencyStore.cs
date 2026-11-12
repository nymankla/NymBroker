using System.Data;
using System.Data.Common;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Idempotency;
using NymBroker.Shared;

namespace NymBroker.Idempotency.MySql;

/// <summary>
/// Durable <see cref="IIdempotencyStore"/> in a MySQL 8.0+ / MariaDB 10.6+ table: duplicates are detected across restarts
/// and across every broker instance that uses the same table. One pooled <see cref="MySqlConnection"/> per operation
/// (not thread-safe).
/// </summary>
public sealed class MySqlIdempotencyStore : IExpiringIdempotencyStore, ITransactionalIdempotencyStore
{
    private readonly MySqlIdempotencySettings _settings;
    private readonly ILogger<MySqlIdempotencyStore> _logger;
    private readonly string _table;
    private readonly string _claimSql;
    private readonly string _completeSql;
    private readonly string _releaseSql;
    private readonly string _deleteExpiredSql;
    private readonly AsyncOnce _schema = new();

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
    }

    public async ValueTask<IdempotencyClaimResult> TryClaimAsync(Guid messageId, CancellationToken ct = default)
    {
        await EnsureSchemaAsync(ct);
        await using var connection = await OpenAsync(ct);
        await using var command = Command(connection, _claimSql, messageId);
        command.Parameters.AddWithValue("@token", Guid.NewGuid().ToByteArray());
        command.Parameters.AddWithValue("@leaseSeconds", _settings.LeaseSeconds);

        await using var reader = await command.ExecuteReaderAsync(ct);
        do
        {
            if (await reader.ReadAsync(ct))
                return ToClaimResult(reader);
        } while (await reader.NextResultAsync(ct));

        // The row vanished between the upsert and the read: let the transport retry.
        return IdempotencyClaimResult.InProgress;
    }

    /// <summary>The claim row read back: column 1 says whether this call took the claim, column 0 is the row's status.</summary>
    private static IdempotencyClaimResult ToClaimResult(MySqlDataReader reader)
    {
        if (reader.GetInt64(1) == 1) return IdempotencyClaimResult.Claimed;
        return reader.GetInt32(0) == MySqlIdempotencySql.Completed ? IdempotencyClaimResult.Duplicate : IdempotencyClaimResult.InProgress;
    }

    public async ValueTask CompleteAsync(Guid messageId, CancellationToken ct = default)
    {
        await EnsureSchemaAsync(ct);
        await using var connection = await OpenAsync(ct);
        await using var command = Command(connection, _completeSql, messageId);
        command.Parameters.AddWithValue("@ttlSeconds", _settings.TtlSeconds);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Marks the message handled in the caller's <see cref="MySqlTransaction"/>, on a table in that database (<see cref="IInbox"/>).</summary>
    public async Task CompleteInTransactionAsync(Guid messageId, DbTransaction transaction, CancellationToken ct = default)
    {
        await EnsureSchemaAsync(ct);
        await TransactionalCompletion.ExecuteAsync<MySqlConnection>(transaction, _completeSql, (DbType.Binary, messageId.ToByteArray()), _settings.TtlSeconds, ct);
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
        return await ExpiringStore.DeleteAllAsync(DeleteBatchAsync, _settings.CleanupBatchSize, ct);
    }

    private async Task<int> DeleteBatchAsync(CancellationToken ct)
    {
        await using var connection = await OpenAsync(ct);
        await using var command = new MySqlCommand(_deleteExpiredSql, connection);
        command.Parameters.AddWithValue("@batchSize", _settings.CleanupBatchSize);
        return await command.ExecuteNonQueryAsync(ct);
    }

    private Task EnsureSchemaAsync(CancellationToken ct)
        => _settings.AutoCreateTable ? _schema.RunAsync(this, static (self, ct) => self.CreateSchemaAsync(ct), ct) : Task.CompletedTask;

    private async Task CreateSchemaAsync(CancellationToken ct)
    {
        await using var connection = await OpenAsync(ct);
        await using var command = new MySqlCommand(MySqlIdempotencySql.CreateSchema(_table), connection);
        await command.ExecuteNonQueryAsync(ct);
        _logger.LogDebug("Idempotency table {Table} is ready", _settings.TableName);
    }

    private Task<MySqlConnection> OpenAsync(CancellationToken ct)
        => PooledConnection.OpenAsync(new MySqlConnection(_settings.ConnectionString), ct);

    private static MySqlCommand Command(MySqlConnection connection, string sql, Guid messageId)
    {
        var command = new MySqlCommand(sql, connection);
        command.Parameters.AddWithValue("@id", messageId.ToByteArray());
        return command;
    }
}
