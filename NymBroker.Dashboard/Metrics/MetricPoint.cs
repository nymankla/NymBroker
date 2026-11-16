namespace NymBroker.Dashboard.Metrics;

/// <summary>The broker counters the dashboard follows, in the order of <see cref="MetricCollector"/>'s accumulators.</summary>
internal enum Counter
{
    Received,
    Consumed,
    Failed,
    DeadLettered,
    Routed,
    Retries,
    Duplicates
}

/// <summary>One second of broker activity. <see cref="T"/> is the Unix time in seconds at which the second ended.</summary>
internal sealed record MetricPoint(long T, long Received, long Consumed, long Failed, long DeadLettered, long Routed, long Retries,
    long Duplicates, double? P50Ms, double? P99Ms, double? MaxAgeSeconds);

/// <summary>One second of one endpoint's activity, by the <c>source</c> tag of the broker's metrics.</summary>
internal sealed record EndpointPoint(long T, long Received, long Failed, long DeadLettered);

/// <summary>Rates per second averaged over the last few seconds, and the latency over the same seconds.</summary>
internal sealed record RecentRates(double Received, double Consumed, double Failed, double DeadLettered, double Routed, double Retries,
    double? P50Ms, double? P99Ms, double? MaxAgeSeconds);

/// <summary>An endpoint's rates per second averaged over the last few seconds.</summary>
internal sealed record EndpointRates(double Received, double Failed, double DeadLettered);

/// <summary>A fixed-size ring: adding beyond the capacity drops the oldest item. Not thread-safe; the collector locks.</summary>
internal sealed class Ring<T>(int capacity)
{
    private readonly T[] _items = new T[capacity];
    private int _next;
    private int _count;

    public int Count => _count;

    public void Add(T item)
    {
        _items[_next] = item;
        _next = (_next + 1) % _items.Length;
        _count = Math.Min(_count + 1, _items.Length);
    }

    /// <summary>The newest <paramref name="max"/> items (all by default), oldest first.</summary>
    public List<T> Latest(int max = int.MaxValue)
    {
        var take = Math.Min(max, _count);
        var result = new List<T>(take);
        for (var i = take; i > 0; i--)
            result.Add(_items[(_next - i + _items.Length) % _items.Length]);
        return result;
    }
}
