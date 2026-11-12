using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Outbox;
using NymBroker.Shared;

namespace NymBroker.Outbox.SqlServer;

/// <summary>
/// <see cref="IOutboxStore"/> in a SQL Server table, shared by every instance of the application. Messages are written in
/// the caller's <see cref="SqlTransaction"/>; the dispatcher uses one pooled connection per operation.
/// </summary>
public sealed class SqlServerOutboxStore : IOutboxStore
{
    private readonly SqlServerOutboxSettings _settings;
    private readonly ILogger<SqlServerOutboxStore> _logger;
    private readonly OutboxStoreCore<SqlConnection> _core;
    private readonly AsyncOnce _schema = new();

    public SqlServerOutboxStore(SqlServerOutboxSettings settings, ILogger<SqlServerOutboxStore> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();
        _settings = settings;
        _logger = logger;
        _core = new OutboxStoreCore<SqlConnection>(new Dialect(SqlServerOutboxSql.Statements(settings.TableName)), OpenAsync, EnsureSchemaAsync);
    }

    public Task AddAsync(DbTransaction transaction, OutboxMessage message, CancellationToken ct = default) => _core.AddAsync(transaction, message, ct);
    public Task<IReadOnlyList<ClaimedOutboxMessage>> ClaimAsync(Guid claimId, int maxCount, TimeSpan leaseTimeout, CancellationToken ct = default) => _core.ClaimAsync(claimId, maxCount, leaseTimeout, ct);
    public Task MarkSentAsync(long id, Guid claimId, CancellationToken ct = default) => _core.MarkSentAsync(id, claimId, ct);
    public Task RetryLaterAsync(long id, Guid claimId, TimeSpan delay, string error, CancellationToken ct = default) => _core.RetryLaterAsync(id, claimId, delay, error, ct);
    public Task MarkFailedAsync(long id, Guid claimId, string error, CancellationToken ct = default) => _core.MarkFailedAsync(id, claimId, error, ct);
    public Task ReleaseAsync(long id, Guid claimId, CancellationToken ct = default) => _core.ReleaseAsync(id, claimId, ct);

    private async Task<OutboxConnection> OpenAsync(CancellationToken ct)
        => new(await PooledConnection.OpenAsync(new SqlConnection(_settings.ConnectionString), ct));

    private Task EnsureSchemaAsync(CancellationToken ct)
        => _settings.AutoCreateTable ? _schema.RunAsync(this, static (self, ct) => self.CreateSchemaAsync(ct), ct) : Task.CompletedTask;

    private async Task CreateSchemaAsync(CancellationToken ct)
    {
        await using var connection = await PooledConnection.OpenAsync(new SqlConnection(_settings.ConnectionString), ct);
        await using var command = new SqlCommand(SqlServerOutboxSql.CreateSchema(_settings.TableName), connection);
        await command.ExecuteNonQueryAsync(ct);
        _logger.LogDebug("Outbox table {Table} is ready", _settings.TableName);
    }

    private sealed class Dialect(OutboxStatements sql) : OutboxDialect(sql)
    {
        public override void AddClaimId(DbCommand command, Guid claimId) => Add(command, "@claimId", DbType.Guid, claimId);

        public override void AddTime(DbCommand command, string name, DateTimeOffset? value) => Add(command, name, DbType.DateTime2, value?.UtcDateTime);

        public override DateTimeOffset ReadTime(DbDataReader reader, int ordinal)
            => new(DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc));
    }
}
