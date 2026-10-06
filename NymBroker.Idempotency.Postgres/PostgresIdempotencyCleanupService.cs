using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace NymBroker.Idempotency.Postgres;

internal sealed class PostgresIdempotencyCleanupService(
    PostgresIdempotencyStore store,
    PostgresIdempotencySettings settings,
    ILogger<PostgresIdempotencyCleanupService> logger) : BackgroundService
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
