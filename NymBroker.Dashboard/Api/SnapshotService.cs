using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using NymBroker.Core;
using NymBroker.Dashboard.Metrics;

namespace NymBroker.Dashboard.Api;

/// <summary>
/// One loop for all open dashboards: while at least one is subscribed it collects a <see cref="LiveSnapshot"/> every
/// <see cref="NymBrokerDashboardOptions.RefreshInterval"/> and hands it to every subscriber. Ten open tabs cost the
/// broker and its databases what one does, and a closed dashboard costs nothing but the metric listener.
/// </summary>
internal sealed class SnapshotService(INymBroker broker, MetricCollector metrics, NymBrokerDashboardOptions options,
    TimeProvider time, ILogger<SnapshotService> logger)
{
    /// <summary>A count that has not answered by then is reported as failed, so one slow database doesn't hold up the page.</summary>
    internal static readonly TimeSpan CountTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Rates and latency are averaged over this many seconds.</summary>
    internal const int RateSeconds = 5;

    private readonly Lock _lock = new();
    private readonly List<Channel<LiveSnapshot>> _subscribers = [];
    private readonly ConcurrentDictionary<string, string> _countErrors = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _collecting = new(1, 1);
    private CancellationTokenSource? _loop;
    private LiveSnapshot? _latest;
    private HealthView? _health;
    private Task? _healthCheck;

    /// <summary>
    /// Receives every snapshot from now on, starting with the latest one if there is one. A slow reader only ever gets
    /// the newest snapshot: older ones are dropped, never queued.
    /// </summary>
    public Subscription Subscribe()
    {
        var channel = Channel.CreateBounded<LiveSnapshot>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropOldest });
        lock (_lock)
        {
            _subscribers.Add(channel);
            if (_latest is not null) channel.Writer.TryWrite(_latest);
            if (_loop is null)
            {
                _loop = new CancellationTokenSource();
                _ = RunLoopAsync(_loop.Token);
            }
        }
        return new Subscription(this, channel);
    }

    private void Unsubscribe(Channel<LiveSnapshot> channel)
    {
        lock (_lock)
        {
            _subscribers.Remove(channel);
            channel.Writer.TryComplete();
            if (_subscribers.Count > 0 || _loop is null) return;
            _loop.Cancel();
            _loop.Dispose();
            _loop = null;
        }
    }

    /// <summary>A subscriber's view of the loop; disposing it stops the loop if it was the last one.</summary>
    internal sealed class Subscription(SnapshotService owner, Channel<LiveSnapshot> channel) : IDisposable
    {
        public ChannelReader<LiveSnapshot> Reader => channel.Reader;
        public void Dispose() => owner.Unsubscribe(channel);
    }

    private async Task RunLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                Publish(await CollectAsync(ct));
                await Task.Delay(options.RefreshInterval, time, ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // The last dashboard closed.
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "The NymBroker dashboard stopped collecting; it starts again when a dashboard is opened");
            ResetLoop();
        }
    }

    private void ResetLoop()
    {
        lock (_lock)
        {
            foreach (var subscriber in _subscribers) subscriber.Writer.TryComplete();
            _subscribers.Clear();
            _loop?.Dispose();
            _loop = null;
        }
    }

    private void Publish(LiveSnapshot snapshot)
    {
        lock (_lock)
        {
            foreach (var subscriber in _subscribers) subscriber.Writer.TryWrite(snapshot);
        }
    }

    /// <summary>The latest snapshot if it is recent, otherwise a new one (for a page that opens between refreshes).</summary>
    public async Task<LiveSnapshot> GetCurrentAsync(CancellationToken ct)
    {
        var latest = _latest;
        if (latest is not null && time.GetUtcNow() - latest.At < options.RefreshInterval * 2) return latest;
        return await CollectAsync(ct);
    }

    /// <summary>Takes one snapshot. Serialized, so the loop and a page opening at the same moment share the work.</summary>
    internal async Task<LiveSnapshot> CollectAsync(CancellationToken ct)
    {
        await _collecting.WaitAsync(ct);
        try
        {
            StartHealthCheckIfDue();
            var description = broker.Describe();
            var endpoints = await Task.WhenAll(description.Endpoints.Select(e => CollectEndpointAsync(e.Name, e.Inspectable, ct)));
            var now = time.GetUtcNow();
            // A window that overlaps the previous snapshot: the page keeps each second once, by its time, so a
            // snapshot taken for another tab (or a late one) never leaves a gap in the charts.
            var since = (now - options.RefreshInterval * 3 - TimeSpan.FromSeconds(5)).ToUnixTimeSeconds();
            var snapshot = new LiveSnapshot(now, description.Started, metrics.Recent(RateSeconds), metrics.History(since), endpoints, _health);
            _latest = snapshot;
            return snapshot;
        }
        finally
        {
            _collecting.Release();
        }
    }

    private async Task<EndpointLive> CollectEndpointAsync(string name, bool inspectable, CancellationToken ct)
    {
        var rates = metrics.RecentFor(name, RateSeconds);
        if (!inspectable || broker.GetQueueInspector(name) is not { } inspector)
            return new EndpointLive(name, rates, null, null, null);

        var started = Stopwatch.GetTimestamp();
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(CountTimeout);
            var counts = await inspector.GetCountsAsync(timeout.Token);
            if (_countErrors.TryRemove(name, out _))
                logger.LogInformation("The NymBroker dashboard can count endpoint '{Endpoint}' again", name);
            return new EndpointLive(name, rates, counts, Stopwatch.GetElapsedTime(started).TotalMilliseconds, null);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            var error = ex is OperationCanceledException ? $"No answer within {CountTimeout.TotalSeconds:0} s" : ex.Message;
            // Logged when the endpoint starts failing, not on every refresh; the page shows the error meanwhile.
            if (_countErrors.TryAdd(name, error))
                logger.LogWarning(ex, "The NymBroker dashboard could not count endpoint '{Endpoint}'", name);
            return new EndpointLive(name, rates, null, Stopwatch.GetElapsedTime(started).TotalMilliseconds, error);
        }
    }

    private void StartHealthCheckIfDue()
    {
        if (_healthCheck is { IsCompleted: false }) return;
        if (_health is not null && time.GetUtcNow() - _health.CheckedAt < options.HealthInterval) return;
        _healthCheck = RefreshHealthAsync();
    }

    /// <summary>Runs beside the snapshots, so a slow endpoint check (up to its timeout) doesn't delay the numbers.</summary>
    private async Task RefreshHealthAsync()
    {
        try
        {
            await CheckHealthAsync();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "The NymBroker dashboard could not check the broker's health; it tries again at the next interval");
        }
    }

    /// <summary>Checks now and keeps the result for the snapshots. <c>CheckHealthAsync</c> never throws except on cancellation.</summary>
    internal async Task<HealthView> CheckHealthAsync(CancellationToken ct = default)
    {
        var report = await broker.CheckHealthAsync(ct);
        return _health = new HealthView(time.GetUtcNow(), report);
    }
}
