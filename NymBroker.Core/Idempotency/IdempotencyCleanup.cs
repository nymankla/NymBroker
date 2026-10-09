using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Idempotency;

/// <summary>
/// A store whose expired entries are removed by a periodic cleanup rather than by itself, typically a database table.
/// Register it with <c>NymBrokerBuilder.AddIdempotencyStore(factory, cleanupInterval, tableName)</c> to get a hosted
/// service that calls <see cref="DeleteExpiredAsync"/> on that interval.
/// </summary>
public interface IExpiringIdempotencyStore : IIdempotencyStore
{
    /// <summary>Deletes entries whose TTL (or abandoned claim's lease) has passed; returns how many were deleted.</summary>
    Task<int> DeleteExpiredAsync(CancellationToken ct = default);
}

internal sealed record IdempotencyCleanupOptions<TStore>(TimeSpan Interval, string TableName)
    where TStore : IExpiringIdempotencyStore;

/// <summary>Calls <see cref="IExpiringIdempotencyStore.DeleteExpiredAsync"/> on the configured interval while the host runs.</summary>
internal sealed class IdempotencyCleanupService<TStore>(
    TStore store,
    IdempotencyCleanupOptions<TStore> options,
    ILogger<IdempotencyCleanupService<TStore>> logger) : BackgroundService
    where TStore : IExpiringIdempotencyStore
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.Interval);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    var deleted = await store.DeleteExpiredAsync(stoppingToken);
                    logger.LogDebug("Deleted {Count} expired idempotency row(s) from {Table}", deleted, options.TableName);
                }
                catch (Exception ex) when (ex is not OperationCanceledException || !stoppingToken.IsCancellationRequested)
                {
                    logger.LogError(ex, "Deleting expired idempotency rows from {Table} failed; retrying at the next interval", options.TableName);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host stopping.
        }
    }
}
