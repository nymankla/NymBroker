using System.Collections.Concurrent;
using MessageBroker.Core.Message;

namespace MessageBroker.Core.Aggregator;

public sealed class AggregatorImpl : IAggregator
{
    private readonly ConcurrentDictionary<Guid, Aggregate> _aggregates = new();
    private readonly ICompletionCondition _completionCondition;

    // Expire incomplete aggregates after this duration.
    private static readonly TimeSpan ExpiryWindow = TimeSpan.FromHours(2);

    public AggregatorImpl(ICompletionCondition? completionCondition = null)
    {
        _completionCondition = completionCondition ?? new SplitMessageCompletionCondition();
    }

    public Task<byte[]?> AddAsync(SplitMessage part, IMessageContext context, CancellationToken ct = default)
    {
        var aggregate = _aggregates.GetOrAdd(part.CorrelationId, id => new Aggregate(id));
        aggregate.Add(part);

        if (!_completionCondition.IsComplete(part, aggregate))
            return Task.FromResult<byte[]?>(null);

        // All parts received — reassemble and clean up.
        _aggregates.TryRemove(part.CorrelationId, out _);
        var result = aggregate.Reassemble();
        PurgeExpired();
        return Task.FromResult<byte[]?>(result);
    }

    private void PurgeExpired()
    {
        var cutoff = DateTime.UtcNow - ExpiryWindow;
        foreach (var key in _aggregates.Keys.ToList())
        {
            if (_aggregates.TryGetValue(key, out var agg) && agg.FirstReceived < cutoff)
                _aggregates.TryRemove(key, out _);
        }
    }
}
