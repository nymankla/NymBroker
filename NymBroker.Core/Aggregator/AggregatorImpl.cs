using System.Collections.Concurrent;
using NymBroker.Core.Message;
using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Aggregator;

public sealed class AggregatorImpl : IAggregator
{
    private readonly ConcurrentDictionary<Guid, Aggregate> _aggregates = new();
    private readonly ICompletionCondition _completionCondition;
    private readonly ILogger<AggregatorImpl> _logger;

    private long _nextPurge;

    private static readonly TimeSpan ExpiryWindow = TimeSpan.FromHours(2);
    private static readonly TimeSpan PurgeInterval = TimeSpan.FromMinutes(1);

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
            if (aggregate.Kept is not null)
            {
                aggregate.IsCompleted = true;
                _aggregates.TryRemove(part.CorrelationId, out _);
                result = aggregate.Kept;
            }
            else if (!aggregate.IsCompleted)
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

        PurgeExpiredIfDue();

        return Task.FromResult(result);
    }

    public void KeepForRetry(Guid correlationId, byte[] reassembled)
        => _aggregates[correlationId] = new Aggregate(correlationId) { Kept = reassembled };

    // On every Add, not only on completion: otherwise groups that never complete stay until another group completes.
    private void PurgeExpiredIfDue()
    {
        var now = Environment.TickCount64;
        var next = Interlocked.Read(ref _nextPurge);
        if (now < next || Interlocked.CompareExchange(ref _nextPurge, now + (long)PurgeInterval.TotalMilliseconds, next) != next)
            return;

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
            _logger.LogWarning("Discarded {Count} split message(s) after {Window}: the missing parts never arrived, or a kept message was not redelivered", purged, ExpiryWindow);
    }
}
