using System.Data;
using System.Data.Common;
using System.Globalization;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Idempotency;
using NymBroker.Shared;

namespace NymBroker.Idempotency.Sqlite;

/// <summary>
/// Durable <see cref="IIdempotencyStore"/> in SQLite: restart-safe deduplication on one host.
/// One persistent connection, serialized (<see cref="SqliteStoreConnection"/>).
/// </summary>
public sealed class SqliteIdempotencyStore : IExpiringIdempotencyStore, ITransactionalIdempotencyStore, IAsyncDisposable
{
    private readonly SqliteIdempotencySettings _settings;
    private readonly SqliteStoreConnection _db;
    private readonly string _claimSql;
    private readonly string _existingStatusSql;
    private readonly string _completeSql;
    private readonly string _releaseSql;
    private readonly string _deleteExpiredSql;

    public SqliteIdempotencyStore(SqliteIdempotencySettings settings, ILogger<SqliteIdempotencyStore> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();
        _settings = settings;
        _db = new SqliteStoreConnection(settings.ConnectionString, settings.AutoCreateTable ? SqliteIdempotencySql.CreateSchema(settings.TableName) : null,
            logger, $"Idempotency table {settings.TableName}");
        _claimSql = SqliteIdempotencySql.Claim(settings.TableName);
        _existingStatusSql = SqliteIdempotencySql.ExistingStatus(settings.TableName);
        _completeSql = SqliteIdempotencySql.Complete(settings.TableName);
        _releaseSql = SqliteIdempotencySql.Release(settings.TableName);
        _deleteExpiredSql = SqliteIdempotencySql.DeleteExpired(settings.TableName);
    }

    public async ValueTask<IdempotencyClaimResult> TryClaimAsync(Guid messageId, CancellationToken ct = default)
    {
        using var db = await _db.AcquireAsync(ct);
        await using var command = CreateCommand(db.Connection, _claimSql, messageId);
        command.Parameters.AddWithValue("@leaseSeconds", _settings.LeaseSeconds);
        if (await command.ExecuteScalarAsync(ct) is not null)
            return IdempotencyClaimResult.Claimed;

        await using var statusCommand = CreateCommand(db.Connection, _existingStatusSql, messageId);
        return Convert.ToInt64(await statusCommand.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) == SqliteIdempotencySql.Completed
            ? IdempotencyClaimResult.Duplicate
            : IdempotencyClaimResult.InProgress;
    }

    public async ValueTask CompleteAsync(Guid messageId, CancellationToken ct = default)
    {
        using var db = await _db.AcquireAsync(ct);
        await using var command = CreateCommand(db.Connection, _completeSql, messageId);
        command.Parameters.AddWithValue("@ttlSeconds", _settings.TtlSeconds);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Marks the message handled in the caller's <see cref="SqliteTransaction"/>, on a table in the same database file
    /// (<see cref="IInbox"/>). The table already exists: the broker claimed the message on the store's own connection.
    /// </summary>
    public Task CompleteInTransactionAsync(Guid messageId, DbTransaction transaction, CancellationToken ct = default)
        => TransactionalCompletion.ExecuteAsync<SqliteConnection>(transaction, _completeSql, (DbType.String, messageId.ToString("D")), _settings.TtlSeconds, ct);

    public async ValueTask ReleaseAsync(Guid messageId, CancellationToken ct = default)
    {
        using var db = await _db.AcquireAsync(ct);
        await using var command = CreateCommand(db.Connection, _releaseSql, messageId);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Deletes expired rows in batches of <see cref="SqliteIdempotencySettings.CleanupBatchSize"/>. Returns the number deleted.</summary>
    public Task<int> DeleteExpiredAsync(CancellationToken ct = default)
        => ExpiringStore.DeleteAllAsync(DeleteBatchAsync, _settings.CleanupBatchSize, ct);

    private async Task<int> DeleteBatchAsync(CancellationToken ct)
    {
        using var db = await _db.AcquireAsync(ct);
        await using var command = db.Connection.CreateCommand();
        command.CommandText = _deleteExpiredSql;
        command.Parameters.AddWithValue("@batchSize", _settings.CleanupBatchSize);
        return await command.ExecuteNonQueryAsync(ct);
    }

    public ValueTask DisposeAsync() => _db.DisposeAsync();

    private static SqliteCommand CreateCommand(SqliteConnection connection, string sql, Guid messageId)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("@id", messageId.ToString("D"));
        return command;
    }
}
