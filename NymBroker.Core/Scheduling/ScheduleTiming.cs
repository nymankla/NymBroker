namespace NymBroker.Core.Scheduling;

/// <summary>When a named scheduled action's occurrences are. Every instance computes the same occurrences, so they can be claimed in a shared store.</summary>
internal abstract class ScheduleTiming
{
    /// <summary>The first occurrence strictly after <paramref name="after"/>, or null if there is none.</summary>
    public abstract DateTimeOffset? Next(DateTimeOffset after);

    /// <summary>The latest occurrence in (<paramref name="after"/>, <paramref name="atOrBefore"/>], or null.</summary>
    public abstract DateTimeOffset? LatestBetween(DateTimeOffset after, DateTimeOffset atOrBefore);

    /// <summary>
    /// The occurrence a <see cref="MissedRunPolicy.RunOnce"/> action catches up on start: the latest one after
    /// <paramref name="lastRun"/> that is not in the future and, with <paramref name="maxCatchUp"/>, not older than that.
    /// </summary>
    public DateTimeOffset? FindMissed(DateTimeOffset lastRun, DateTimeOffset now, TimeSpan? maxCatchUp)
    {
        var after = lastRun;
        if (maxCatchUp is { } max && now - max > after)
            after = now - max;
        return LatestBetween(after, now);
    }
}

/// <summary>Cron occurrences in the local time zone.</summary>
internal sealed class CronTiming(CronSchedule cron) : ScheduleTiming
{
    public override DateTimeOffset? Next(DateTimeOffset after) => cron.GetNextOccurrence(after, TimeZoneInfo.Local);

    public override DateTimeOffset? LatestBetween(DateTimeOffset after, DateTimeOffset atOrBefore)
    {
        DateTimeOffset? latest = null;
        for (var next = Next(after); next is { } n && n <= atOrBefore; next = Next(n))
            latest = n;
        return latest;
    }

    public override string ToString() => cron.ToString();
}

/// <summary>
/// Interval occurrences on a fixed grid: multiples of the interval since 1970-01-01 UTC, so instances started at
/// different times agree on them. The interval is whole milliseconds, so occurrences are too.
/// </summary>
internal sealed class GridTiming : ScheduleTiming
{
    private readonly long _intervalTicks;

    public GridTiming(TimeSpan interval)
    {
        if (interval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(interval), "The interval must be positive.");
        if (interval.Ticks % TimeSpan.TicksPerMillisecond != 0)
            throw new ArgumentOutOfRangeException(nameof(interval), "The interval of a named scheduled action must be a whole number of milliseconds.");
        _intervalTicks = interval.Ticks;
    }

    public override DateTimeOffset? Next(DateTimeOffset after) => GridPoint(Floor(after) + 1);

    public override DateTimeOffset? LatestBetween(DateTimeOffset after, DateTimeOffset atOrBefore)
    {
        var latest = GridPoint(Floor(atOrBefore));
        return latest > after ? latest : null;
    }

    private long Floor(DateTimeOffset time) => (time.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) / _intervalTicks;

    private DateTimeOffset GridPoint(long index) => DateTimeOffset.UnixEpoch.AddTicks(index * _intervalTicks);

    public override string ToString() => TimeSpan.FromTicks(_intervalTicks).ToString();
}
