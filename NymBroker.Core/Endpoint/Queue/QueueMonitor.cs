using System.Collections.Frozen;
using System.Collections.Immutable;
using System.Diagnostics.Metrics;
using System.Globalization;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Diagnostics;

namespace NymBroker.Core.Endpoint.Queue;

/// <summary>
/// Samples every endpoint with an <see cref="IQueueInspector"/> (#133): the counts per state and the oldest pending and
/// failed message. The queue gauges and the health check read the last sample, so neither queries a database itself.
/// </summary>
internal sealed class QueueMonitor(
    QueueMonitoringOptions options,
    Func<IEnumerable<KeyValuePair<string, IEndPoint>>> endpoints,
    Func<string?> deadLetterEndpoint,
    ILogger logger,
    TimeProvider? timeProvider = null)
{
    private static ImmutableList<QueueMonitor> s_published = [];

    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;
    private FrozenDictionary<string, QueueSample> _samples = FrozenDictionary<string, QueueSample>.Empty;

    public QueueMonitoringOptions Options { get; } = options;

    /// <summary>The last sample of each endpoint; an endpoint whose last sample failed is left out.</summary>
    public IReadOnlyDictionary<string, QueueSample> Samples => _samples;

    /// <summary>Samples until <paramref name="ct"/> is cancelled: at once, then every <see cref="QueueMonitoringOptions.Interval"/>.</summary>
    public async Task RunAsync(CancellationToken ct)
    {
        WarnAboutUninspectableThresholds();
        using var published = Publish();
        using var timer = new PeriodicTimer(Options.Interval, _time);
        try
        {
            do await SampleAsync(ct);
            while (await timer.WaitForNextTickAsync(ct));
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }

    /// <summary>Makes this monitor's samples visible to the queue gauges until disposed.</summary>
    public IDisposable Publish()
    {
        _ = NymBrokerDiagnostics.QueueMessages;   // creates the gauges, which read the published monitors
        ImmutableInterlocked.Update(ref s_published, list => list.Add(this));
        return new Unpublish(this);
    }

    private sealed class Unpublish(QueueMonitor monitor) : IDisposable
    {
        public void Dispose() => ImmutableInterlocked.Update(ref s_published, list => list.Remove(monitor));
    }

    /// <summary>Samples every inspectable endpoint once; a failing endpoint is logged and left out of the samples.</summary>
    public async Task SampleAsync(CancellationToken ct)
    {
        var samples = new Dictionary<string, QueueSample>(StringComparer.OrdinalIgnoreCase);
        foreach (var (name, endpoint) in endpoints())
        {
            if (endpoint is not IQueueInspector inspector) continue;
            try
            {
                samples[name] = await SampleEndpointAsync(inspector, ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
            {
                logger.LogWarning(ex, "Could not sample queue endpoint '{Endpoint}'; its queue metrics are left out until the next sample", name);
            }
        }
        _samples = samples.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
    }

    private static async Task<QueueSample> SampleEndpointAsync(IQueueInspector inspector, CancellationToken ct)
    {
        var counts = await inspector.GetCountsAsync(ct);
        var oldestPending = counts.Pending > 0 ? await OldestAsync(inspector, QueueInspectionState.Pending, ct) : null;
        var oldestFailed = counts.Failed > 0 ? await OldestAsync(inspector, QueueInspectionState.Failed, ct) : null;
        return new QueueSample(counts, oldestPending, oldestFailed);
    }

    /// <summary>When the oldest message in <paramref name="state"/> was posted, or failed for a failed one.</summary>
    private static async Task<DateTimeOffset?> OldestAsync(IQueueInspector inspector, QueueInspectionState state, CancellationToken ct)
    {
        var page = await inspector.GetMessagesAsync(state, limit: 1, ct: ct);
        return page.Messages is [var oldest, ..] ? oldest.FailedAt ?? oldest.CreatedAt : null;
    }

    /// <summary>
    /// Why <paramref name="endpointName"/> is over its dead-letter threshold, or null when it is not (or has no threshold
    /// or sample). On the broker's dead-letter endpoint every waiting message is a dead letter; elsewhere the failed ones.
    /// </summary>
    public string? DeadLetterBreach(string endpointName)
    {
        if (!Options.DeadLetterThresholds.TryGetValue(endpointName, out var threshold)
            || !_samples.TryGetValue(endpointName, out var sample))
            return null;

        var (count, oldestAt) = DeadLetters(endpointName, sample);
        var age = oldestAt is { } at ? _time.GetUtcNow() - at : (TimeSpan?)null;
        if (!(count > threshold.MaxCount) && !(age > threshold.MaxAge)) return null;

        var limit = Describe(threshold);
        return age is { } oldest
            ? $"{count} dead letter(s) ({limit}), oldest {Hours(oldest)}"
            : $"{count} dead letter(s) ({limit})";
    }

    /// <summary>On the broker's dead-letter endpoint every message there; elsewhere the failed ones.</summary>
    private (long Count, DateTimeOffset? OldestAt) DeadLetters(string endpointName, QueueSample sample)
        => string.Equals(endpointName, deadLetterEndpoint(), StringComparison.OrdinalIgnoreCase)
            ? (sample.Counts.Active + sample.Counts.Failed, Earliest(sample.OldestPendingAt, sample.OldestFailedAt))
            : (sample.Counts.Failed, sample.OldestFailedAt);

    private static string Describe(DeadLetterThreshold threshold) => (threshold.MaxCount, threshold.MaxAge) switch
    {
        ({ } max, { } maxAge) => $"threshold {max}, or older than {Hours(maxAge)}",
        ({ } max, null) => $"threshold {max}",
        _ => $"threshold older than {Hours(threshold.MaxAge!.Value)}"
    };

    private static DateTimeOffset? Earliest(DateTimeOffset? a, DateTimeOffset? b)
        => a is { } x && b is { } y ? (x < y ? x : y) : a ?? b;

    private static string Hours(TimeSpan time)
        => time.TotalHours >= 2   // "65 min" says more than "1.1 h"
            ? time.TotalHours.ToString("0.0", CultureInfo.InvariantCulture) + " h"
            : time.TotalMinutes.ToString("0", CultureInfo.InvariantCulture) + " min";

    private void WarnAboutUninspectableThresholds()
    {
        var inspectable = endpoints().Where(e => e.Value is IQueueInspector).Select(e => e.Key).ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var name in Options.DeadLetterThresholds.Keys.Where(n => !inspectable.Contains(n)))
            logger.LogWarning(
                "Endpoint '{Endpoint}' has a dead-letter threshold but its transport offers no queue inspector, so the threshold is never checked",
                name);
    }

    // --- Gauge callbacks (NymBrokerDiagnostics) ---

    internal static IEnumerable<Measurement<long>> ObserveMessages()
    {
        foreach (var monitor in s_published)
            foreach (var (name, sample) in monitor._samples)
            {
                var endpoint = new KeyValuePair<string, object?>("endpoint", name);
                yield return new(sample.Counts.Pending, endpoint, new("state", "pending"));
                yield return new(sample.Counts.InProgress, endpoint, new("state", "in_progress"));
                yield return new(sample.Counts.Deferred, endpoint, new("state", "deferred"));
                yield return new(sample.Counts.Failed, endpoint, new("state", "failed"));
            }
    }

    internal static IEnumerable<Measurement<double>> ObserveOldestAge()
        => s_published.SelectMany(monitor => monitor.OldestAges());

    private IEnumerable<Measurement<double>> OldestAges()
    {
        var now = _time.GetUtcNow();
        foreach (var (name, sample) in _samples)
        {
            if (sample.OldestPendingAt is { } pending) yield return Age(now, pending, name, "pending");
            if (sample.OldestFailedAt is { } failed) yield return Age(now, failed, name, "failed");
        }
    }

    private static Measurement<double> Age(DateTimeOffset now, DateTimeOffset since, string endpoint, string state)
        => new(Math.Max(0, (now - since).TotalSeconds), new("endpoint", endpoint), new("state", state));
}

/// <summary>One endpoint's sample: its counts and when its oldest pending message was posted and its oldest failed one failed.</summary>
internal sealed record QueueSample(QueueCounts Counts, DateTimeOffset? OldestPendingAt, DateTimeOffset? OldestFailedAt);
