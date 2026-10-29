using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Scheduling;

namespace NymBroker.Scheduling.SqlServer;

/// <summary>
/// Durable <see cref="IScheduleStore"/> in a SQL Server table, shared by every broker instance that uses it: each
/// occurrence of a named scheduled action runs once, and missed runs can be caught up after a restart.
/// One pooled <see cref="SqlConnection"/> per operation (not thread-safe).
/// </summary>
public sealed class SqlServerScheduleStore : IScheduleStore
{
    private readonly SqlServerScheduleStoreSettings _settings;
    private readonly ILogger<SqlServerScheduleStore> _logger;
    private readonly string _getLastRunSql;
    private readonly string _claimSql;
    private readonly AsyncOnce _schema = new();

    public SqlServerScheduleStore(SqlServerScheduleStoreSettings settings, ILogger<SqlServerScheduleStore> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();
        _settings = settings;
        _logger = logger;
        _getLastRunSql = SqlServerScheduleSql.GetLastRun(settings.TableName);
        _claimSql = SqlServerScheduleSql.Claim(settings.TableName);
    }

    public async ValueTask<DateTimeOffset?> GetLastRunAsync(string name, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(name);
        await EnsureSchemaAsync(ct);
        await using var connection = await OpenAsync(ct);
        await using var command = Command(connection, _getLastRunSql, name);
        return await command.ExecuteScalarAsync(ct) is DateTime lastRun
            ? new DateTimeOffset(DateTime.SpecifyKind(lastRun, DateTimeKind.Utc))
            : null;
    }

    public async ValueTask<bool> TryClaimAsync(string name, DateTimeOffset occurrence, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(name);
        await EnsureSchemaAsync(ct);
        await using var connection = await OpenAsync(ct);
        await using var command = Command(connection, _claimSql, name);
        command.Parameters.Add("@occurrence", SqlDbType.DateTime2).Value = occurrence.UtcDateTime;
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct)) == 1;
    }

    private Task EnsureSchemaAsync(CancellationToken ct)
        => _settings.AutoCreateTable ? _schema.RunAsync(this, static (self, ct) => self.CreateSchemaAsync(ct), ct) : Task.CompletedTask;

    private async Task CreateSchemaAsync(CancellationToken ct)
    {
        await using var connection = await OpenAsync(ct);
        await using var command = new SqlCommand(SqlServerScheduleSql.CreateSchema(_settings.TableName), connection);
        await command.ExecuteNonQueryAsync(ct);
        _logger.LogDebug("Schedule table {Table} is ready", _settings.TableName);
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

    private static SqlCommand Command(SqlConnection connection, string sql, string name)
    {
        var command = new SqlCommand(sql, connection);
        command.Parameters.Add("@name", SqlDbType.NVarChar, 200).Value = name;
        return command;
    }
}
