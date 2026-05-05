using System.Collections.Concurrent;
using MessageBroker.Core.Message;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Core.Aggregator;

public sealed class AggregatorImpl : IAggregator
{
    private readonly ConcurrentDictionary<Guid, Aggregate> _aggregates = new();
    private readonly ICompletionCondition _completionCondition;
    private readonly ILogger<AggregatorImpl> _logger;

    private static readonly TimeSpan ExpiryWindow = TimeSpan.FromHours(2);

    public AggregatorImpl(ILogger<AggregatorImpl> logger, ICompletionCondition? completionCondition = null)
    {
        _logger = logger;
        _completionCondition = completionCondition ?? new SplitMessageCompletionCondition();
    }

    public Task<byte[]?> AddAsync(SplitMessage part, IMessageContext context, CancellationToken ct = default)
    {
        var aggregate = _aggregates.GetOrAdd(part.CorrelationId, id => new Aggregate(id));
        byte[]? result = null;

        lock (aggregate)
        {
            if (!aggregate.IsCompleted)
            {
                aggregate.Add(part);
                if (_completionCondition.IsComplete(part, aggregate))
                {
                    aggregate.IsCompleted = true;
                    _aggregates.TryRemove(part.CorrelationId, out _);
                    result = aggregate.Reassemble();
                }
            }
        }

        if (result != null)
            PurgeExpired();

        return Task.FromResult(result);
    }

    private void PurgeExpired()
    {
        var cutoff = DateTime.UtcNow - ExpiryWindow;
        var purged = 0;
        foreach (var key in _aggregates.Keys.ToList())
        {
            if (_aggregates.TryGetValue(key, out var agg) && agg.FirstReceived < cutoff)
            {
                _aggregates.TryRemove(key, out _);
                purged++;
            }
        }
        if (purged > 0)
            _logger.LogDebug("Purged {Count} expired aggregate(s)", purged);
    }
}
