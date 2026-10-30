namespace NymBroker.Core.Scheduling;

/// <summary>In-process <see cref="IScheduleStore"/>, the default: runs are remembered until the process exits.</summary>
internal sealed class InMemoryScheduleStore : IScheduleStore
{
    private readonly Dictionary<string, DateTimeOffset> _lastRuns = new(StringComparer.OrdinalIgnoreCase);
    private readonly Lock _lock = new();

    public ValueTask<DateTimeOffset?> GetLastRunAsync(string name, CancellationToken ct = default)
    {
        lock (_lock)
            return ValueTask.FromResult<DateTimeOffset?>(_lastRuns.TryGetValue(name, out var last) ? last : null);
    }

    public ValueTask<bool> TryClaimAsync(string name, DateTimeOffset occurrence, CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (_lastRuns.TryGetValue(name, out var last) && last >= occurrence)
                return ValueTask.FromResult(false);
            _lastRuns[name] = occurrence.ToUniversalTime();
            return ValueTask.FromResult(true);
        }
    }
}
