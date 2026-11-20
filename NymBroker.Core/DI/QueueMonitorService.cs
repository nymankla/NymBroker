using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Impl;

namespace NymBroker.Core.DI;

/// <summary>Runs the broker's queue monitor (<c>MonitorQueues</c>, #133) while the host runs.</summary>
internal sealed class QueueMonitorService(NymBrokerImpl broker, ILogger<QueueMonitorService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (broker.QueueMonitor is not { } monitor) return;
        try
        {
            await monitor.RunAsync(stoppingToken);
        }
        catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
        {
            logger.LogCritical(ex, "The queue monitor stopped unexpectedly; queue metrics and dead-letter thresholds are no longer updated");
        }
    }
}
