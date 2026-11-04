// CA2208: the name reported is the settings property that is wrong, not this method's parameter.
#pragma warning disable CA2208

namespace NymBroker.Shared;

/// <summary>What the idempotency stores have in common, whichever database they use.</summary>
internal static class ExpiringStore
{
    internal static void ValidateTimes(TimeSpan ttl, TimeSpan leaseTimeout, TimeSpan cleanupInterval, int cleanupBatchSize)
    {
        if (ttl < TimeSpan.FromSeconds(1) || ttl.TotalSeconds > int.MaxValue)
            throw new ArgumentException("Ttl must be at least 1 second.", "Ttl");
        if (leaseTimeout < TimeSpan.FromSeconds(1) || leaseTimeout.TotalSeconds > int.MaxValue)
            throw new ArgumentException("LeaseTimeout must be at least 1 second.", "LeaseTimeout");
        if (cleanupInterval < TimeSpan.Zero)
            throw new ArgumentException("CleanupInterval cannot be negative.", "CleanupInterval");
        if (cleanupBatchSize < 1)
            throw new ArgumentException("CleanupBatchSize must be at least 1.", "CleanupBatchSize");
    }

    /// <summary>Runs <paramref name="deleteBatch"/> until a batch comes back short, and returns the number deleted.</summary>
    internal static async Task<int> DeleteAllAsync(Func<CancellationToken, Task<int>> deleteBatch, int batchSize, CancellationToken ct)
    {
        var total = 0;
        while (true)
        {
            var deleted = await deleteBatch(ct);
            total += deleted;
            if (deleted < batchSize)
                return total;
        }
    }
}
