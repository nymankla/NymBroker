using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace NymBroker.Dashboard.Metrics;

/// <summary>Starts the <see cref="MetricCollector"/> with the host and closes one second of metrics every second.</summary>
internal sealed class DashboardMetricsService(MetricCollector collector, TimeProvider time, ILogger<DashboardMetricsService> logger)
    : BackgroundService
{
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        collector.Start();
        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1), time);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
                RollSafely();
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host stopping.
        }
    }

    private void RollSafely()
    {
        try
        {
            collector.Roll(time.GetUtcNow().ToUnixTimeSeconds());
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "The NymBroker dashboard could not record a second of metrics; it tries again next second");
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        collector.Dispose();
    }
}
