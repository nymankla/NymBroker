using System.Collections.Immutable;

namespace NymBroker.Core.Endpoint.Queue;

/// <summary>
/// Options for <c>NymBrokerBuilder.MonitorQueues</c>: how often every endpoint with an <see cref="IQueueInspector"/> is
/// sampled for the queue gauges, and when its dead letters make the health check report it Degraded.
/// </summary>
public sealed class QueueMonitoringOptions
{
    /// <summary>Default for <see cref="Interval"/>.</summary>
    public static readonly TimeSpan DefaultInterval = TimeSpan.FromSeconds(60);

    private TimeSpan _interval = DefaultInterval;
    private ImmutableDictionary<string, DeadLetterThreshold> _thresholds =
        ImmutableDictionary.Create<string, DeadLetterThreshold>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Time between samples (default 60 s, at least 1 s). Each sample runs one count query per inspectable endpoint, and
    /// one more for each non-empty pending or failed state, to find its oldest message.
    /// </summary>
    public TimeSpan Interval
    {
        get => _interval;
        set
        {
            if (value < TimeSpan.FromSeconds(1))
                throw new ArgumentOutOfRangeException(nameof(value), value, "The queue sampling interval must be at least 1 second.");
            _interval = value;
        }
    }

    /// <summary>The dead-letter thresholds, by endpoint name (case-insensitive).</summary>
    public IReadOnlyDictionary<string, DeadLetterThreshold> DeadLetterThresholds => _thresholds;

    /// <summary>
    /// Reports <paramref name="endpointName"/> Degraded in the health check when it holds more than
    /// <paramref name="maxCount"/> dead letters, or its oldest dead letter is older than <paramref name="maxAge"/>. Its dead
    /// letters are its failed messages; on the broker's dead-letter endpoint (<c>WithDeadLetterEndpoint</c>), every message
    /// waiting there. Give at least one limit; calling it again for the same endpoint replaces the threshold.
    /// </summary>
    public QueueMonitoringOptions DeadLetterThreshold(string endpointName, long? maxCount = null, TimeSpan? maxAge = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpointName);
        if (maxCount is null && maxAge is null)
            throw new ArgumentException("Give maxCount, maxAge or both.", nameof(maxCount));
        if (maxCount < 0)
            throw new ArgumentOutOfRangeException(nameof(maxCount), maxCount, "maxCount cannot be negative.");
        if (maxAge <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(maxAge), maxAge, "maxAge must be positive.");

        _thresholds = _thresholds.SetItem(endpointName, new DeadLetterThreshold(maxCount, maxAge));
        return this;
    }
}

/// <summary>When an endpoint's dead letters make it Degraded (see <see cref="QueueMonitoringOptions.DeadLetterThreshold"/>).</summary>
/// <param name="MaxCount">Degraded above this many dead letters; null = no count limit.</param>
/// <param name="MaxAge">Degraded when the oldest dead letter is older; null = no age limit.</param>
public sealed record DeadLetterThreshold(long? MaxCount, TimeSpan? MaxAge);
