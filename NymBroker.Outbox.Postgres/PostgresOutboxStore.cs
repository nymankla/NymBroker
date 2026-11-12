using System.Data;
using System.Data.Common;
using Microsoft.Extensions.Logging;
using Npgsql;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Outbox;
using NymBroker.Shared;

namespace NymBroker.Outbox.Postgres;

/// <summary>
/// <see cref="IOutboxStore"/> in a PostgreSQL table, shared by every instance of the application. Messages are written in
/// the caller's <see cref="NpgsqlTransaction"/>; the dispatcher opens pooled connections from a thread-safe data source.
/// </summary>
public sealed class PostgresOutboxStore : IOutboxStore, IAsyncDisposable
{
    private readonly PostgresOutboxSettings _settings;
    private readonly ILogger<PostgresOutboxStore> _logger;
    private readonly NpgsqlDataSource _dataSource;
    private readonly OutboxStoreCore<NpgsqlConnection> _core;
    private readonly AsyncOnce _schema = new();

    public PostgresOutboxStore(PostgresOutboxSettings settings, ILogger<PostgresOutboxStore> logger)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();
        _settings = settings;
        _logger = logger;
        _dataSource = NpgsqlDataSource.Create(settings.ConnectionString);
        _core = new OutboxStoreCore<NpgsqlConnection>(new Dialect(PostgresOutboxSql.Statements(settings.TableName)), OpenAsync, EnsureSchemaAsync);
    }

    public Task AddAsync(DbTransaction transaction, OutboxMessage message, CancellationToken ct = default) => _core.AddAsync(transaction, message, ct);
    public Task<IReadOnlyList<ClaimedOutboxMessage>> ClaimAsync(Guid claimId, int maxCount, TimeSpan leaseTimeout, CancellationToken ct = default) => _core.ClaimAsync(claimId, maxCount, leaseTimeout, ct);
    public Task MarkSentAsync(long id, Guid claimId, CancellationToken ct = default) => _core.MarkSentAsync(id, claimId, ct);
    public Task RetryLaterAsync(long id, Guid claimId, TimeSpan delay, string error, CancellationToken ct = default) => _core.RetryLaterAsync(id, claimId, delay, error, ct);
    public Task MarkFailedAsync(long id, Guid claimId, string error, CancellationToken ct = default) => _core.MarkFailedAsync(id, claimId, error, ct);
    public Task ReleaseAsync(long id, Guid claimId, CancellationToken ct = default) => _core.ReleaseAsync(id, claimId, ct);

    public ValueTask DisposeAsync() => _dataSource.DisposeAsync();

    private async Task<OutboxConnection> OpenAsync(CancellationToken ct) => new(await _dataSource.OpenConnectionAsync(ct));

    private Task EnsureSchemaAsync(CancellationToken ct)
        => _settings.AutoCreateTable ? _schema.RunAsync(this, static (self, ct) => self.CreateSchemaAsync(ct), ct) : Task.CompletedTask;

    private async Task CreateSchemaAsync(CancellationToken ct)
    {
        await PostgresStoreSchema.CreateAsync(_dataSource, PostgresOutboxSql.CreateSchema(_settings.TableName), _settings.TableName, ct);
        _logger.LogDebug("Outbox table {Table} is ready", _settings.TableName);
    }

    private sealed class Dialect(OutboxStatements sql) : OutboxDialect(sql)
    {
        public override void AddClaimId(DbCommand command, Guid claimId) => Add(command, "@claimId", DbType.Guid, claimId);

        // DateTimeOffset maps to timestamptz, which Npgsql requires in UTC.
        public override void AddTime(DbCommand command, string name, DateTimeOffset? value) => Add(command, name, DbType.DateTimeOffset, value?.ToUniversalTime());

        public override DateTimeOffset ReadTime(DbDataReader reader, int ordinal) => reader.GetFieldValue<DateTimeOffset>(ordinal);
    }
}
