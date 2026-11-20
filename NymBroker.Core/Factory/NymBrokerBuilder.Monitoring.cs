using NymBroker.Core.Endpoint.Queue;

namespace NymBroker.Core.Factory;

public sealed partial class NymBrokerBuilder
{
    private QueueMonitoringOptions? _queueMonitoring;

    /// <summary>
    /// Samples every endpoint that has an <see cref="IQueueInspector"/> (the SQL queue tables) in the background, at start
    /// and then every <see cref="QueueMonitoringOptions.Interval"/>, and publishes the counts and oldest ages as the gauges
    /// <c>nymbroker.queue.messages</c> and <c>nymbroker.queue.oldest_message.age</c>. With
    /// <see cref="QueueMonitoringOptions.DeadLetterThreshold"/>, the health check reports an endpoint Degraded while its
    /// dead letters are over the threshold. Gauges and the health check read the last sample; neither queries the database.
    /// Calling it again replaces the options.
    /// </summary>
    public NymBrokerBuilder MonitorQueues(Action<QueueMonitoringOptions>? configure = null)
    {
        var options = new QueueMonitoringOptions();
        configure?.Invoke(options);
        _queueMonitoring = options;
        return this;
    }

    private void ValidateQueueMonitoring()
    {
        if (_queueMonitoring is null) return;
        var unknown = _queueMonitoring.DeadLetterThresholds.Keys
            .Where(n => !_endpoints.Contains(n, StringComparer.OrdinalIgnoreCase))
            .ToList();
        if (unknown.Count > 0)
            throw new InvalidOperationException(
                $"MonitorQueues has a dead-letter threshold for unknown endpoint(s): {string.Join(", ", unknown)}. Register the endpoint, or remove the threshold.");
    }
}
