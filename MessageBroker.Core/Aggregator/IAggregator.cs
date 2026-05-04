using MessageBroker.Core.Message;

namespace MessageBroker.Core.Aggregator;

public interface IAggregator
{
    /// <summary>
    /// Adds a split-message part. Returns the reassembled payload when all parts have arrived,
    /// or null if the aggregate is still incomplete.
    /// </summary>
    Task<byte[]?> AddAsync(SplitMessage part, IMessageContext context, CancellationToken ct = default);
}
