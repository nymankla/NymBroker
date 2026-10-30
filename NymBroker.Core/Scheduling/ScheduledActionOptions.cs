namespace NymBroker.Core.Scheduling;

/// <summary>Options of a named scheduled action (<c>NymBrokerBuilder.AddScheduledAction(name, …)</c>).</summary>
public sealed class ScheduledActionOptions
{
    /// <summary>What to do on start about occurrences missed while the broker was not running. Default <see cref="MissedRunPolicy.Skip"/>.</summary>
    public MissedRunPolicy MissedRuns { get; set; } = MissedRunPolicy.Skip;

    /// <summary>
    /// With <see cref="MissedRunPolicy.RunOnce"/>: only catch up an occurrence missed at most this long ago. Null (the
    /// default) catches up a missed occurrence of any age.
    /// </summary>
    public TimeSpan? MaxCatchUp { get; set; }

    internal void Validate()
    {
        if (!Enum.IsDefined(MissedRuns))
            throw new ArgumentOutOfRangeException(nameof(MissedRuns), MissedRuns, "Unknown MissedRunPolicy.");
        if (MaxCatchUp is { } max && max <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(MaxCatchUp), max, "MaxCatchUp must be positive.");
    }
}
