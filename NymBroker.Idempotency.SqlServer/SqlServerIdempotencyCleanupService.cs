using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace NymBroker.Idempotency.SqlServer;

/// <summary>Deletes expired idempotency rows every <see cref="SqlServerIdempotencySettings.CleanupInterval"/> while the host runs.</summary>
internal sealed class SqlServerIdempotencyCleanupService(
    SqlServerIdempotencyStore store,
    SqlServerIdempotencySettings settings,
    ILogger<SqlServerIdempotencyCleanupService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(settings.CleanupInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    var deleted = await store.DeleteExpiredAsync(stoppingToken);
                    logger.LogDebug("Deleted {Count} expired idempotency row(s) from {Table}", deleted, settings.TableName);
                }
                catch (Exception ex) when (ex is not OperationCanceledException || !stoppingToken.IsCancellationRequested)
                {
                    logger.LogError(ex, "Deleting expired idempotency rows from {Table} failed; retrying at the next interval", settings.TableName);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host stopping.
        }
    }
}
