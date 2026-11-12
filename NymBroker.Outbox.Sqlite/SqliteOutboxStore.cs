using System.Data;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Outbox;
using NymBroker.Shared;

namespace NymBroker.Outbox.Sqlite;

/// <summary>
/// <see cref="IOutboxStore"/> in a SQLite database file: a transactional outbox for one host. Messages are written in the
/// caller's <see cref="SqliteTransaction"/>; the dispatcher uses one persistent connection, serialized
/// (<see cref="SqliteStoreConnection"/>).
/// </summary>
public sealed class SqliteOutboxStore : IOutboxStore, IAsyncDisposable
{
    private readonly SqliteStoreConnection _db;
    private readonly OutboxStoreCore<SqliteConnection> _core;

    public SqliteOutboxStore(SqliteOutboxSettings settings, ILogger<SqliteOutboxStore> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();
        var createSchema = settings.AutoCreateTable ? SqliteOutboxSql.CreateSchema(settings.TableName) : null;
        _db = new SqliteStoreConnection(settings.ConnectionString, createSchema, logger, $"Outbox table {settings.TableName}");
        // The dispatcher's connection creates the table when it opens, so there is nothing else to ensure.
        _core = new OutboxStoreCore<SqliteConnection>(new Dialect(SqliteOutboxSql.Statements(settings.TableName)), OpenAsync,
            static _ => Task.CompletedTask, createSchema);
    }

    public Task AddAsync(DbTransaction transaction, OutboxMessage message, CancellationToken ct = default) => _core.AddAsync(transaction, message, ct);
    public Task<IReadOnlyList<ClaimedOutboxMessage>> ClaimAsync(Guid claimId, int maxCount, TimeSpan leaseTimeout, CancellationToken ct = default) => _core.ClaimAsync(claimId, maxCount, leaseTimeout, ct);
    public Task MarkSentAsync(long id, Guid claimId, CancellationToken ct = default) => _core.MarkSentAsync(id, claimId, ct);
    public Task RetryLaterAsync(long id, Guid claimId, TimeSpan delay, string error, CancellationToken ct = default) => _core.RetryLaterAsync(id, claimId, delay, error, ct);
    public Task MarkFailedAsync(long id, Guid claimId, string error, CancellationToken ct = default) => _core.MarkFailedAsync(id, claimId, error, ct);
    public Task ReleaseAsync(long id, Guid claimId, CancellationToken ct = default) => _core.ReleaseAsync(id, claimId, ct);

    public ValueTask DisposeAsync() => _db.DisposeAsync();

    private async Task<OutboxConnection> OpenAsync(CancellationToken ct)
    {
        var lease = await _db.AcquireAsync(ct);
        return new OutboxConnection(lease.Connection, lease);
    }

    private sealed class Dialect(OutboxStatements sql) : OutboxDialect(sql)
    {
        public override void AddClaimId(DbCommand command, Guid claimId) => Add(command, "@claimId", DbType.String, claimId.ToString("D"));

        public override void AddTime(DbCommand command, string name, DateTimeOffset? value) => Add(command, name, DbType.Int64, value?.ToUnixTimeMilliseconds());

        public override DateTimeOffset ReadTime(DbDataReader reader, int ordinal) => DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64(ordinal));
    }
}
