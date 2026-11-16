using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Diagnostics.Metrics;

namespace NymBroker.Dashboard.Metrics;

/// <summary>
/// Follows the broker's own metrics in-process, through a <see cref="MeterListener"/>, and keeps a short history for
/// the charts. Measurements only add to the current second (interlocked, no lock); <see cref="Roll"/>, called once a
/// second, moves that second into bounded rings. Memory is fixed by <see cref="NymBrokerDashboardOptions.History"/>.
/// </summary>
internal sealed class MetricCollector : IDisposable
{
    internal const string DurationInstrument = "nymbroker.message.processing.duration";
    internal const string AgeInstrument = "nymbroker.message.age";
    private const int RecentHistograms = 10;
    private const int MaxEndpoints = 256;

    internal static readonly FrozenSet<string> MeterNames = FrozenSet.ToFrozenSet(["NymBroker", "NymBroker.Resilience"], StringComparer.Ordinal);

    internal static readonly FrozenDictionary<string, Counter> CounterInstruments = new Dictionary<string, Counter>
    {
        ["nymbroker.messages.received"] = Counter.Received,
        ["nymbroker.messages.consumed"] = Counter.Consumed,
        ["nymbroker.messages.failed"] = Counter.Failed,
        ["nymbroker.messages.dead_lettered"] = Counter.DeadLettered,
        ["nymbroker.messages.routed"] = Counter.Routed,
        ["nymbroker.retries"] = Counter.Retries,
        ["nymbroker.messages.duplicates"] = Counter.Duplicates,
    }.ToFrozenDictionary(StringComparer.Ordinal);

    private static readonly int CounterCount = Enum.GetValues<Counter>().Length;

    // The current second, written by measurement callbacks on any thread.
    private readonly long[] _current = new long[CounterCount];
    private readonly LogHistogram _durations = new();
    private long _maxAgeBits = BitConverter.DoubleToInt64Bits(-1);
    private readonly ConcurrentDictionary<string, long[]> _endpointCurrent = new(StringComparer.Ordinal);

    // History, written by Roll and read by the API; guarded by _lock.
    private readonly Lock _lock = new();
    private readonly int _capacity;
    private readonly Ring<MetricPoint> _points;
    private readonly Ring<long[]> _recentDurations = new(RecentHistograms);
    private readonly Dictionary<string, Ring<EndpointPoint>> _endpoints = new(StringComparer.Ordinal);

    private MeterListener? _listener;

    public MetricCollector(NymBrokerDashboardOptions options)
    {
        _capacity = (int)options.History.TotalSeconds;
        _points = new Ring<MetricPoint>(_capacity);
    }

    /// <summary>Starts listening; measurements recorded before this are not seen.</summary>
    public void Start()
    {
        if (_listener is not null) return;
        _listener = new MeterListener { InstrumentPublished = Subscribe };
        _listener.SetMeasurementEventCallback<long>(static (instrument, value, tags, state) => ((Subscription)state!).OnLong(value, tags));
        _listener.SetMeasurementEventCallback<double>(static (instrument, value, tags, state) => ((Subscription)state!).OnDouble(value));
        _listener.Start();
    }

    public void Dispose() => _listener?.Dispose();

    private void Subscribe(Instrument instrument, MeterListener listener)
    {
        if (!MeterNames.Contains(instrument.Meter.Name)) return;
        if (CounterInstruments.TryGetValue(instrument.Name, out var counter))
            listener.EnableMeasurementEvents(instrument, new Subscription(this, counter, null));
        else if (instrument.Name is DurationInstrument or AgeInstrument)
            listener.EnableMeasurementEvents(instrument, new Subscription(this, null, instrument.Name));
    }

    /// <summary>What a measurement of one instrument is added to.</summary>
    private sealed class Subscription(MetricCollector collector, Counter? counter, string? histogram)
    {
        public void OnLong(long value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            if (counter is { } c) collector.Add(c, value, SourceOf(tags));
        }

        public void OnDouble(double value)
        {
            if (histogram == DurationInstrument) collector.RecordDuration(value);
            else collector.RecordAge(value);
        }
    }

    private static string? SourceOf(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        foreach (var tag in tags)
        {
            if (tag.Key == "source") return tag.Value as string;
        }
        return null;
    }

    internal void Add(Counter counter, long value, string? source)
    {
        Interlocked.Add(ref _current[(int)counter], value);
        var slot = EndpointSlot(counter);
        if (slot < 0 || source is null) return;
        if (_endpointCurrent.TryGetValue(source, out var perEndpoint) || TryTrack(source, out perEndpoint))
            Interlocked.Add(ref perEndpoint[slot], value);
    }

    /// <summary>Adds an endpoint the first time it is seen, up to <see cref="MaxEndpoints"/>, so a flood of tag values can't grow memory.</summary>
    private bool TryTrack(string source, out long[] perEndpoint)
    {
        perEndpoint = _endpointCurrent.Count < MaxEndpoints ? _endpointCurrent.GetOrAdd(source, static _ => new long[3]) : [];
        return perEndpoint.Length == 3;
    }

    private static int EndpointSlot(Counter counter) => counter switch
    {
        Counter.Received => 0,
        Counter.Failed => 1,
        Counter.DeadLettered => 2,
        _ => -1
    };

    internal void RecordDuration(double milliseconds) => _durations.Record(milliseconds);

    internal void RecordAge(double seconds)
    {
        var current = Interlocked.Read(ref _maxAgeBits);
        while (seconds > BitConverter.Int64BitsToDouble(current))
        {
            var seen = Interlocked.CompareExchange(ref _maxAgeBits, BitConverter.DoubleToInt64Bits(seconds), current);
            if (seen == current) return;
            current = seen;
        }
    }

    /// <summary>Closes the current second: moves its counts into the history as the second ending at <paramref name="unixSecond"/>.</summary>
    public void Roll(long unixSecond)
    {
        var counts = new long[CounterCount];
        for (var i = 0; i < counts.Length; i++) counts[i] = Interlocked.Exchange(ref _current[i], 0);
        var histogram = new long[LogHistogram.BucketCount];
        _durations.DrainInto(histogram);
        var maxAge = BitConverter.Int64BitsToDouble(Interlocked.Exchange(ref _maxAgeBits, BitConverter.DoubleToInt64Bits(-1)));

        var point = new MetricPoint(unixSecond, counts[0], counts[1], counts[2], counts[3], counts[4], counts[5], counts[6],
            LogHistogram.Percentile(histogram, 0.5), LogHistogram.Percentile(histogram, 0.99), maxAge < 0 ? null : maxAge);
        var endpointPoints = DrainEndpoints(unixSecond);

        lock (_lock)
        {
            _points.Add(point);
            _recentDurations.Add(histogram);
            foreach (var (name, endpointPoint) in endpointPoints)
                EndpointRing(name).Add(endpointPoint);
        }
    }

    private List<(string Name, EndpointPoint Point)> DrainEndpoints(long unixSecond)
    {
        var result = new List<(string, EndpointPoint)>(_endpointCurrent.Count);
        foreach (var (name, counts) in _endpointCurrent)
        {
            result.Add((name, new EndpointPoint(unixSecond,
                Interlocked.Exchange(ref counts[0], 0), Interlocked.Exchange(ref counts[1], 0), Interlocked.Exchange(ref counts[2], 0))));
        }
        return result;
    }

    private Ring<EndpointPoint> EndpointRing(string name)
    {
        if (!_endpoints.TryGetValue(name, out var ring))
            _endpoints[name] = ring = new Ring<EndpointPoint>(_capacity);
        return ring;
    }

    /// <summary>Every second in the history, oldest first, or only those after <paramref name="afterUnixSecond"/>.</summary>
    public List<MetricPoint> History(long afterUnixSecond = long.MinValue)
    {
        lock (_lock) return [.. _points.Latest().Where(p => p.T > afterUnixSecond)];
    }

    /// <summary>Rates per second over the last <paramref name="seconds"/> seconds (at most 10 for the latency).</summary>
    public RecentRates Recent(int seconds)
    {
        List<MetricPoint> points;
        List<long[]> histograms;
        lock (_lock)
        {
            points = _points.Latest(seconds);
            histograms = _recentDurations.Latest(seconds);
        }

        var merged = new long[LogHistogram.BucketCount];
        foreach (var histogram in histograms)
            for (var i = 0; i < merged.Length; i++) merged[i] += histogram[i];

        double Rate(Func<MetricPoint, long> value) => points.Count == 0 ? 0 : (double)points.Sum(value) / points.Count;
        return new RecentRates(Rate(p => p.Received), Rate(p => p.Consumed), Rate(p => p.Failed), Rate(p => p.DeadLettered),
            Rate(p => p.Routed), Rate(p => p.Retries), LogHistogram.Percentile(merged, 0.5), LogHistogram.Percentile(merged, 0.99),
            points.Max(p => p.MaxAgeSeconds));
    }

    /// <summary>An endpoint's rates per second over the last <paramref name="seconds"/> seconds; zero when it has had no traffic.</summary>
    public EndpointRates RecentFor(string endpoint, int seconds)
    {
        List<EndpointPoint> points;
        int window;
        lock (_lock)
        {
            points = _endpoints.TryGetValue(endpoint, out var ring) ? ring.Latest(seconds) : [];
            // Over the same seconds as the broker-wide rates: an endpoint first seen a second ago has one point, not five.
            window = Math.Min(seconds, _points.Count);
        }
        if (points.Count == 0 || window == 0) return new EndpointRates(0, 0, 0);
        return new EndpointRates((double)points.Sum(p => p.Received) / window,
            (double)points.Sum(p => p.Failed) / window, (double)points.Sum(p => p.DeadLettered) / window);
    }
}
