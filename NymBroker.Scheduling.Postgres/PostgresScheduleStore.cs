using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Scheduling;
using NymBroker.Shared;

namespace NymBroker.Scheduling.Postgres;

/// <summary>
/// Durable <see cref="IScheduleStore"/> in PostgreSQL, shared by all broker instances using the same table: each
/// occurrence of a named scheduled action runs once, and missed runs can be caught up after a restart.
/// Each operation opens a pooled connection from a thread-safe data source.
/// </summary>
public sealed class PostgresScheduleStore : IScheduleStore, IAsyncDisposable
{
    private readonly PostgresScheduleStoreSettings _settings;
    private readonly ILogger<PostgresScheduleStore> _logger;
    private readonly NpgsqlDataSource _dataSource;
    private readonly string _getLastRunSql;
    private readonly string _claimSql;
    private readonly AsyncOnce _schema = new();

    public PostgresScheduleStore(PostgresScheduleStoreSettings settings, ILogger<PostgresScheduleStore> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();
        _settings = settings;
        _logger = logger;
        _dataSource = NpgsqlDataSource.Create(settings.ConnectionString);
        _getLastRunSql = PostgresScheduleSql.GetLastRun(settings.TableName);
        _claimSql = PostgresScheduleSql.Claim(settings.TableName);
    }

    public async ValueTask<DateTimeOffset?> GetLastRunAsync(string name, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(name);
        await EnsureSchemaAsync(ct);
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(_getLastRunSql, connection);
        command.Parameters.AddWithValue("name", name);
        return await command.ExecuteScalarAsync(ct) is DateTime lastRun
            ? new DateTimeOffset(DateTime.SpecifyKind(lastRun, DateTimeKind.Utc))
            : null;
    }

    public async ValueTask<bool> TryClaimAsync(string name, DateTimeOffset occurrence, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(name);
        await EnsureSchemaAsync(ct);
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(_claimSql, connection);
        command.Parameters.AddWithValue("name", name);
        command.Parameters.Add(new NpgsqlParameter<DateTime>("occurrence", NpgsqlDbType.TimestampTz) { TypedValue = occurrence.UtcDateTime });
        return await command.ExecuteScalarAsync(ct) is not null;
    }

    public async ValueTask DisposeAsync()
    {
        await _dataSource.DisposeAsync();
    }

    private Task EnsureSchemaAsync(CancellationToken ct)
        => _settings.AutoCreateTable ? _schema.RunAsync(this, static (self, ct) => self.CreateSchemaAsync(ct), ct) : Task.CompletedTask;

    private async Task CreateSchemaAsync(CancellationToken ct)
    {
        await PostgresStoreSchema.CreateAsync(_dataSource, PostgresScheduleSql.CreateSchema(_settings.TableName), _settings.TableName, ct);
        _logger.LogDebug("Schedule table {Table} is ready", _settings.TableName);
    }
}
