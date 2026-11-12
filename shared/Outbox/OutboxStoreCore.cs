using System.Data;
using System.Data.Common;
using NymBroker.Core.Outbox;

namespace NymBroker.Shared;

/// <summary>A connection for one store operation: a pooled one disposed afterwards, or a lease on a shared one.</summary>
internal sealed class OutboxConnection(DbConnection connection, IDisposable? lease = null) : IAsyncDisposable
{
    public DbConnection Connection => connection;

    public ValueTask DisposeAsync()
    {
        if (lease is null) return connection.DisposeAsync();
        lease.Dispose();
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// What every <see cref="IOutboxStore"/> package does, whichever database it uses: the insert in the caller's
/// transaction, and the dispatcher's claim and outcome writes on the store's own connection.
/// </summary>
/// <param name="open">Opens the store's own connection.</param>
/// <param name="ensureSchema">Creates the table if needed (and allowed), on the store's own connection.</param>
/// <param name="createInTransaction">SQL that creates the table in the caller's transaction before the insert (SQLite), or null.</param>
internal sealed class OutboxStoreCore<TConnection>(
    OutboxDialect dialect,
    Func<CancellationToken, Task<OutboxConnection>> open,
    Func<CancellationToken, Task> ensureSchema,
    string? createInTransaction = null) where TConnection : DbConnection
{
    public async Task AddAsync(DbTransaction transaction, OutboxMessage message, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(message);
        var connection = Joinable(transaction);
        if (createInTransaction is null)
        {
            await ensureSchema(ct);
        }
        else
        {
            await using var create = OutboxDialect.Command(connection, createInTransaction, transaction);
            await create.ExecuteNonQueryAsync(ct);
        }

        await using var command = OutboxDialect.Command(connection, dialect.Sql.Insert, transaction);
        OutboxDialect.Add(command, "@endpoint", DbType.String, message.EndpointName);
        OutboxDialect.Add(command, "@body", DbType.Binary, message.Body);
        dialect.AddTime(command, "@deliverAt", message.DeliverAt);
        await command.ExecuteNonQueryAsync(ct);
    }

    public async Task<IReadOnlyList<ClaimedOutboxMessage>> ClaimAsync(Guid claimId, int maxCount, TimeSpan leaseTimeout, CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxCount, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(leaseTimeout, TimeSpan.FromSeconds(1));
        await ensureSchema(ct);
        await using var connection = await open(ct);
        return await dialect.ClaimAsync(connection.Connection, claimId, maxCount, (int)Math.Ceiling(leaseTimeout.TotalSeconds), ct);
    }

    public Task MarkSentAsync(long id, Guid claimId, CancellationToken ct)
        => ExecuteClaimedAsync(dialect.Sql.MarkSent, id, claimId, null, ct);

    public Task RetryLaterAsync(long id, Guid claimId, TimeSpan delay, string error, CancellationToken ct)
        => ExecuteClaimedAsync(dialect.Sql.RetryLater, id, claimId, command =>
        {
            OutboxDialect.Add(command, "@delayMs", DbType.Int32, (int)Math.Clamp(Math.Ceiling(delay.TotalMilliseconds), 0, int.MaxValue));
            OutboxDialect.Add(command, "@error", DbType.String, error);
        }, ct);

    public Task MarkFailedAsync(long id, Guid claimId, string error, CancellationToken ct)
        => ExecuteClaimedAsync(dialect.Sql.MarkFailed, id, claimId, command => OutboxDialect.Add(command, "@error", DbType.String, error), ct);

    public Task ReleaseAsync(long id, Guid claimId, CancellationToken ct)
        => ExecuteClaimedAsync(dialect.Sql.Release, id, claimId, null, ct);

    private async Task ExecuteClaimedAsync(string sql, long id, Guid claimId, Action<DbCommand>? addParameters, CancellationToken ct)
    {
        await ensureSchema(ct);
        await using var connection = await open(ct);
        await using var command = OutboxDialect.Command(connection.Connection, sql);
        OutboxDialect.Add(command, "@id", DbType.Int64, id);
        dialect.AddClaimId(command, claimId);
        addParameters?.Invoke(command);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static TConnection Joinable(DbTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        return transaction.Connection switch
        {
            TConnection connection => connection,
            null => throw new ArgumentException("The transaction has already been committed or rolled back.", nameof(transaction)),
            var other => throw new ArgumentException(
                $"The outbox table is written in the caller's transaction, so it must be a {typeof(TConnection).Name} transaction, not {other.GetType().Name}.",
                nameof(transaction))
        };
    }
}
