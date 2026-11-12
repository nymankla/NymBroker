using System.Data.Common;

namespace NymBroker.Core.Outbox;

/// <summary>
/// Posts messages through a transactional outbox: the envelope is written in the application's own database transaction
/// and sent by a background dispatcher after the transaction commits, so a message is sent if and only if the business
/// change it belongs to is committed. Registered by an outbox package (for example <c>AddSqlServerOutbox</c>).
/// </summary>
/// <remarks>
/// Delivery is at least once, not exactly once: a crash after a send and before the outbox row is removed sends the
/// message again with the same envelope id, so let the receiver drop duplicates (an idempotent receiver). The envelope,
/// and so the message id, is created by this call; output transformers run when the message is sent.
/// </remarks>
public interface IOutbox
{
    /// <summary>
    /// Writes <paramref name="message"/> for <paramref name="endpointName"/> in <paramref name="transaction"/>. It is sent
    /// once the transaction commits, and never if it rolls back.
    /// </summary>
    /// <param name="transaction">An open transaction on a connection to the outbox table's database.</param>
    /// <exception cref="InvalidOperationException">No endpoint is registered with that name, or it is read-only.</exception>
    /// <exception cref="ArgumentException"><paramref name="transaction"/> has ended, or is for another kind of database, or the message is a byte array or stream.</exception>
    Task PostAsync<T>(string endpointName, T message, DbTransaction transaction, CancellationToken ct = default) where T : class;

    /// <summary>
    /// As <see cref="PostAsync{T}"/>, but not delivered before <paramref name="deliverAt"/>: the message waits in the
    /// outbox table, so the wait is durable for every endpoint. Its envelope's <c>created</c> is the due time, as for
    /// <c>INymBroker.PostDeferredAsync</c>.
    /// </summary>
    Task PostDeferredAsync<T>(string endpointName, T message, DateTimeOffset deliverAt, DbTransaction transaction, CancellationToken ct = default) where T : class;

    /// <summary>Writes bytes as-is (an existing envelope, or raw input for an input transformer), as <c>INymBroker.PostRawAsync</c> sends them.</summary>
    Task PostRawAsync(string endpointName, ReadOnlyMemory<byte> message, DbTransaction transaction, CancellationToken ct = default);
}
