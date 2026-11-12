using System.Data.Common;

namespace NymBroker.Core.Outbox;

/// <summary>
/// The outbox table behind <see cref="IOutbox"/>, implemented by the <c>NymBroker.Outbox.*</c> packages and registered with
/// <c>NymBrokerBuilder.AddOutboxStore</c>. A row is written in the application's transaction, claimed with a lease by
/// the dispatcher once committed (rows of transactions still open are skipped, not waited for), and removed once sent.
/// Every write after the claim names the claim, so a dispatcher whose lease expired cannot change a row another one has
/// claimed since.
/// </summary>
public interface IOutboxStore
{
    /// <summary>Inserts a row in <paramref name="transaction"/>, due now or at <see cref="OutboxMessage.DeliverAt"/>.</summary>
    /// <exception cref="ArgumentException">The transaction is not one this store's database can join.</exception>
    Task AddAsync(DbTransaction transaction, OutboxMessage message, CancellationToken ct = default);

    /// <summary>
    /// Claims up to <paramref name="maxCount"/> due rows, oldest first, for <paramref name="leaseTimeout"/>: until then no
    /// other claim returns them. Counts the attempt. Returns them ordered by <see cref="ClaimedOutboxMessage.Id"/>.
    /// </summary>
    Task<IReadOnlyList<ClaimedOutboxMessage>> ClaimAsync(Guid claimId, int maxCount, TimeSpan leaseTimeout, CancellationToken ct = default);

    /// <summary>The message was sent: removes the row.</summary>
    Task MarkSentAsync(long id, Guid claimId, CancellationToken ct = default);

    /// <summary>The send failed: the row is due again after <paramref name="delay"/>, with <paramref name="error"/> recorded.</summary>
    Task RetryLaterAsync(long id, Guid claimId, TimeSpan delay, string error, CancellationToken ct = default);

    /// <summary>The send failed for good: the row stays, with <paramref name="error"/>, and is not claimed again.</summary>
    Task MarkFailedAsync(long id, Guid claimId, string error, CancellationToken ct = default);

    /// <summary>The message was not sent (the dispatcher is stopping): the row is due again now and the attempt does not count.</summary>
    Task ReleaseAsync(long id, Guid claimId, CancellationToken ct = default);
}
