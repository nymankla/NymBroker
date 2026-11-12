using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;
using NymBroker.Core.Message;

namespace NymBroker.Core.Idempotency;

/// <summary>
/// An idempotency store that can record a message as handled inside the application's own transaction, so the record and
/// the consumer's writes commit or roll back together. The table must be in the database the transaction is on.
/// </summary>
public interface ITransactionalIdempotencyStore : IIdempotencyStore
{
    /// <summary>
    /// Marks <paramref name="messageId"/> handled (as <see cref="IIdempotencyStore.CompleteAsync"/> does) in
    /// <paramref name="transaction"/>: the record exists if and only if the transaction commits.
    /// </summary>
    /// <exception cref="ArgumentException">The transaction is not one this store's database can join.</exception>
    Task CompleteInTransactionAsync(Guid messageId, DbTransaction transaction, CancellationToken ct = default);
}

/// <summary>
/// Closes the window between a consumer's commit and the broker recording the message as handled: a crash in between
/// otherwise has the message processed again when it is redelivered. Inject it into a consumer and call
/// <see cref="MarkHandledAsync"/> in the consumer's transaction before committing. Needs an idempotent receiver whose
/// store implements <see cref="ITransactionalIdempotencyStore"/> (the <c>NymBroker.Idempotency.*</c> stores do), with
/// its table in the same database as the consumer's data.
/// </summary>
public interface IInbox
{
    /// <summary>
    /// Records the message being consumed as handled in <paramref name="transaction"/>. If the transaction rolls back, the
    /// record does too, and the message is processed again when it is redelivered. A message without an id
    /// (<see cref="Guid.Empty"/>) is not checked for duplicates by the broker, so nothing is written.
    /// </summary>
    /// <exception cref="InvalidOperationException">No idempotency store is registered, or it cannot join a transaction.</exception>
    Task MarkHandledAsync(IMessageContext context, DbTransaction transaction, CancellationToken ct = default);
}

internal sealed class Inbox(IServiceProvider services) : IInbox
{
    public Task MarkHandledAsync(IMessageContext context, DbTransaction transaction, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(transaction);
        if (transaction.Connection is null)
            throw new ArgumentException("The transaction has already been committed or rolled back.", nameof(transaction));
        if (context.Id == Guid.Empty)
            return Task.CompletedTask;

        var store = services.GetService<IIdempotencyStore>()
            ?? throw new InvalidOperationException(
                "IInbox needs an idempotent receiver: register a database idempotency store, for example AddSqlServerIdempotency.");
        if (store is not ITransactionalIdempotencyStore transactional)
            throw new InvalidOperationException(
                $"The idempotency store {store.GetType().Name} cannot join a transaction; use a NymBroker.Idempotency.* store, or implement ITransactionalIdempotencyStore.");

        return transactional.CompleteInTransactionAsync(context.Id, transaction, ct);
    }
}
