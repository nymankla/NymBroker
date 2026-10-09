using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using NymBroker.Core.Idempotency;

namespace NymBroker.Idempotency.Postgres;

/// <summary>
/// Durable <see cref="IIdempotencyStore"/> in PostgreSQL, shared by all broker instances using the same table.
/// Each operation opens a pooled connection from a thread-safe data source.
/// </summary>
public sealed class PostgresIdempotencyStore : IExpiringIdempotencyStore, IAsyncDisposable
{
    private readonly PostgresIdempotencySettings _settings;
    private readonly ILogger<PostgresIdempotencyStore> _logger;
    private readonly NpgsqlDataSource _dataSource;
    private readonly string _claimSql;
    private readonly string _existingStatusSql;
    private readonly string _completeSql;
    private readonly string _releaseSql;
    private readonly string _deleteExpiredSql;
    private readonly SemaphoreSlim _schemaLock = new(1, 1);
    private volatile bool _schemaReady;

    public PostgresIdempotencyStore(PostgresIdempotencySettings settings, ILogger<PostgresIdempotencyStore> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();
        _settings = settings;
        _logger = logger;
        _dataSource = NpgsqlDataSource.Create(settings.ConnectionString);
        _claimSql = PostgresIdempotencySql.Claim(settings.TableName);
        _existingStatusSql = PostgresIdempotencySql.ExistingStatus(settings.TableName);
        _completeSql = PostgresIdempotencySql.Complete(settings.TableName);
        _releaseSql = PostgresIdempotencySql.Release(settings.TableName);
        _deleteExpiredSql = PostgresIdempotencySql.DeleteExpired(settings.TableName);
        _schemaReady = !settings.AutoCreateTable;
    }

    public async ValueTask<IdempotencyClaimResult> TryClaimAsync(Guid messageId, CancellationToken ct = default)
    {
        await EnsureSchemaAsync(ct);
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(_claimSql, connection);
        AddGuid(command, messageId);
        command.Parameters.AddWithValue("leaseSeconds", _settings.LeaseSeconds);

        if (await command.ExecuteScalarAsync(ct) is not null)
            return IdempotencyClaimResult.Claimed;

        await using var statusCommand = new NpgsqlCommand(_existingStatusSql, connection);
        AddGuid(statusCommand, messageId);
        return Convert.ToInt16(await statusCommand.ExecuteScalarAsync(ct)) == PostgresIdempotencySql.Completed
            ? IdempotencyClaimResult.Duplicate
            : IdempotencyClaimResult.InProgress;
    }

    public async ValueTask CompleteAsync(Guid messageId, CancellationToken ct = default)
    {
        await EnsureSchemaAsync(ct);
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(_completeSql, connection);
        AddGuid(command, messageId);
        command.Parameters.AddWithValue("ttlSeconds", _settings.TtlSeconds);
        await command.ExecuteNonQueryAsync(ct);
    }

    public async ValueTask ReleaseAsync(Guid messageId, CancellationToken ct = default)
    {
        await EnsureSchemaAsync(ct);
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(_releaseSql, connection);
        AddGuid(command, messageId);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Deletes expired rows in batches of <see cref="PostgresIdempotencySettings.CleanupBatchSize"/>. Returns the number deleted.</summary>
    public async Task<int> DeleteExpiredAsync(CancellationToken ct = default)
    {
        await EnsureSchemaAsync(ct);
        var total = 0;
        while (true)
        {
            await using var connection = await _dataSource.OpenConnectionAsync(ct);
            await using var command = new NpgsqlCommand(_deleteExpiredSql, connection);
            command.Parameters.AddWithValue("batchSize", _settings.CleanupBatchSize);
            var deleted = await command.ExecuteNonQueryAsync(ct);
            total += deleted;
            if (deleted < _settings.CleanupBatchSize)
                return total;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _dataSource.DisposeAsync();
        _schemaLock.Dispose();
    }

    private async Task EnsureSchemaAsync(CancellationToken ct)
    {
        if (_schemaReady) return;

        await _schemaLock.WaitAsync(ct);
        try
        {
            if (_schemaReady) return;
            await using var connection = await _dataSource.OpenConnectionAsync(ct);
            await using var command = new NpgsqlCommand(PostgresIdempotencySql.CreateSchema(_settings.TableName), connection);
            await command.ExecuteNonQueryAsync(ct);
            _schemaReady = true;
            _logger.LogDebug("Idempotency table {Table} is ready", _settings.TableName);
        }
        finally
        {
            _schemaLock.Release();
        }
    }

    private static void AddGuid(NpgsqlCommand command, Guid messageId)
        => command.Parameters.Add(new NpgsqlParameter<Guid>("id", NpgsqlDbType.Uuid) { TypedValue = messageId });
}
