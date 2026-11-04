using NymBroker.Core.Message;

namespace NymBroker.Core.Aggregator;

public interface IAggregator
{
    /// <summary>
    /// Adds a split-message part. Returns the reassembled payload when all parts have arrived,
    /// or null if the aggregate is still incomplete.
    /// </summary>
    Task<byte[]?> AddAsync(SplitMessage part, IMessageContext context, CancellationToken ct = default);

    /// <summary>
    /// Keeps a reassembled payload whose processing ended in Retry: the transport redelivers only the part that completed
    /// the group, because the others are already settled. The next <see cref="AddAsync"/> for this group returns it at once.
    /// Default: nothing is kept, so that redelivered part starts a new, incomplete group.
    /// </summary>
    void KeepForRetry(Guid correlationId, byte[] reassembled) { }
}
