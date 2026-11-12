using System.Data;
using System.Data.Common;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Outbox;
using NymBroker.Shared;

namespace NymBroker.Outbox.MySql;

/// <summary>
/// <see cref="IOutboxStore"/> in a MySQL 8.0+ / MariaDB 10.6+ table, shared by every instance of the application. Messages
/// are written in the caller's <see cref="MySqlTransaction"/>; the dispatcher uses one pooled connection per operation.
/// </summary>
public sealed class MySqlOutboxStore : IOutboxStore
{
    private readonly MySqlOutboxSettings _settings;
    private readonly ILogger<MySqlOutboxStore> _logger;
    private readonly string _table;
    private readonly OutboxStoreCore<MySqlConnection> _core;
    private readonly AsyncOnce _schema = new();

    public MySqlOutboxStore(MySqlOutboxSettings settings, ILogger<MySqlOutboxStore> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();
        _settings = settings;
        _logger = logger;
        _table = MySqlIdentifier.QuoteTableName(settings.TableName);
        _core = new OutboxStoreCore<MySqlConnection>(new Dialect(_table), OpenAsync, EnsureSchemaAsync);
    }

    public Task AddAsync(DbTransaction transaction, OutboxMessage message, CancellationToken ct = default) => _core.AddAsync(transaction, message, ct);
    public Task<IReadOnlyList<ClaimedOutboxMessage>> ClaimAsync(Guid claimId, int maxCount, TimeSpan leaseTimeout, CancellationToken ct = default) => _core.ClaimAsync(claimId, maxCount, leaseTimeout, ct);
    public Task MarkSentAsync(long id, Guid claimId, CancellationToken ct = default) => _core.MarkSentAsync(id, claimId, ct);
    public Task RetryLaterAsync(long id, Guid claimId, TimeSpan delay, string error, CancellationToken ct = default) => _core.RetryLaterAsync(id, claimId, delay, error, ct);
    public Task MarkFailedAsync(long id, Guid claimId, string error, CancellationToken ct = default) => _core.MarkFailedAsync(id, claimId, error, ct);
    public Task ReleaseAsync(long id, Guid claimId, CancellationToken ct = default) => _core.ReleaseAsync(id, claimId, ct);

    private async Task<OutboxConnection> OpenAsync(CancellationToken ct)
        => new(await PooledConnection.OpenAsync(new MySqlConnection(_settings.ConnectionString), ct));

    private Task EnsureSchemaAsync(CancellationToken ct)
        => _settings.AutoCreateTable ? _schema.RunAsync(this, static (self, ct) => self.CreateSchemaAsync(ct), ct) : Task.CompletedTask;

    private async Task CreateSchemaAsync(CancellationToken ct)
    {
        await using var connection = await PooledConnection.OpenAsync(new MySqlConnection(_settings.ConnectionString), ct);
        await using var command = new MySqlCommand(MySqlOutboxSql.CreateSchema(_table), connection);
        await command.ExecuteNonQueryAsync(ct);
        _logger.LogDebug("Outbox table {Table} is ready", _settings.TableName);
    }

    private sealed class Dialect(string table) : OutboxDialect(MySqlOutboxSql.Statements(table))
    {
        public override void AddClaimId(DbCommand command, Guid claimId) => Add(command, "@claimId", DbType.Binary, claimId.ToByteArray());

        public override void AddTime(DbCommand command, string name, DateTimeOffset? value) => Add(command, name, DbType.DateTime, value?.UtcDateTime);

        public override DateTimeOffset ReadTime(DbDataReader reader, int ordinal)
            => new(DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc));

        /// <summary>Two round trips on one connection: lock the due ids, then claim them and commit.</summary>
        public override async Task<IReadOnlyList<ClaimedOutboxMessage>> ClaimAsync(
            DbConnection connection, Guid claimId, int maxCount, int leaseSeconds, CancellationToken ct)
        {
            var ids = await LockDueAsync(connection, maxCount, ct);
            if (ids.Count == 0)
            {
                await using var commit = Command(connection, "COMMIT;");
                await commit.ExecuteNonQueryAsync(ct);
                return [];
            }

            await using var claim = Command(connection, MySqlOutboxSql.Claim(table, ids));
            AddClaimId(claim, claimId);
            Add(claim, "@leaseSeconds", DbType.Int32, leaseSeconds);
            return await ReadClaimedAsync(claim, ct);
        }

        private async Task<List<long>> LockDueAsync(DbConnection connection, int maxCount, CancellationToken ct)
        {
            await using var command = Command(connection, Sql.Claim);
            Add(command, "@batchSize", DbType.Int32, maxCount);
            var ids = new List<long>();
            await using var reader = await command.ExecuteReaderAsync(ct);
            do
            {
                while (await reader.ReadAsync(ct))
                    ids.Add(reader.GetInt64(0));
            } while (await reader.NextResultAsync(ct));
            return ids;
        }
    }
}
