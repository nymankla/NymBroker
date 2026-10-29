namespace NymBroker.Core.Scheduling;

/// <summary>What a named scheduled action does on start about occurrences it missed while the broker was not running.</summary>
public enum MissedRunPolicy
{
    /// <summary>Ignore missed occurrences; wait for the next one.</summary>
    Skip = 0,

    /// <summary>If one or more occurrences were missed since the last recorded run, run once right away, then follow the schedule.</summary>
    RunOnce = 1
}
