using System.Data.Common;
using NymBroker.Core.Impl;

namespace NymBroker.Core.Outbox;

/// <summary>Builds the envelope with the broker, as a post would, and writes it with the store in the caller's transaction.</summary>
internal sealed class Outbox(NymBrokerImpl broker, IOutboxStore store) : IOutbox
{
    public Task PostAsync<T>(string endpointName, T message, DbTransaction transaction, CancellationToken ct = default) where T : class
        => AddAsync(transaction, broker.CreateOutboxMessage(endpointName, message, deliverAt: null), ct);

    public Task PostDeferredAsync<T>(string endpointName, T message, DateTimeOffset deliverAt, DbTransaction transaction, CancellationToken ct = default) where T : class
        => AddAsync(transaction, broker.CreateOutboxMessage(endpointName, message, deliverAt), ct);

    public Task PostRawAsync(string endpointName, ReadOnlyMemory<byte> message, DbTransaction transaction, CancellationToken ct = default)
        => AddAsync(transaction, broker.CreateRawOutboxMessage(endpointName, message), ct);

    private Task AddAsync(DbTransaction transaction, OutboxMessage message, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        if (transaction.Connection is null)
            throw new ArgumentException("The transaction has already been committed or rolled back.", nameof(transaction));
        return store.AddAsync(transaction, message, ct);
    }
}
