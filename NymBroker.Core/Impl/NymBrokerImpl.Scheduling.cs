using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Scheduling;

namespace NymBroker.Core.Impl;

internal sealed partial class NymBrokerImpl
{
    /// <summary>Longest single wait of a schedule loop; <see cref="Task.Delay(TimeSpan, CancellationToken)"/> rejects delays over ~49 days.</summary>
    internal static readonly TimeSpan MaxScheduleDelay = TimeSpan.FromDays(1);

    // Where named scheduled actions claim their occurrences. Replaced by the builder before StartAsync.
    private IScheduleStore _scheduleStore = new InMemoryScheduleStore();

    internal void SetScheduleStore(IScheduleStore store) => _scheduleStore = store;

    public INymBroker AddScheduledAction(TimeSpan timeSpan, Action action)
    {
        ArgumentNullException.ThrowIfNull(action);
        return AddScheduledAction(timeSpan, _ => { action(); return Task.CompletedTask; });
    }

    public INymBroker AddScheduledAction<T1>(string expression, Action<T1> action, T1 param1)
    {
        ArgumentNullException.ThrowIfNull(action);
        return AddScheduledAction(expression, _ => { action(param1); return Task.CompletedTask; });
    }

    public INymBroker AddScheduledAction(TimeSpan interval, Func<CancellationToken, Task> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        return RegisterScheduledAction(ct => StartIntervalScheduledAction(interval, interval, action, ct));
    }

    public INymBroker AddScheduledAction(string expression, Func<CancellationToken, Task> action)
    {
        ArgumentException.ThrowIfNullOrEmpty(expression);
        ArgumentNullException.ThrowIfNull(action);

        var timing = new CronTiming(CronSchedule.Parse(expression));
        _logger.LogInformation("Scheduled cron action set up, first occurrence at {NextOccurrence}", timing.Next(DateTimeOffset.Now));
        return RegisterScheduledAction(ct => StartTimedScheduledAction(timing, (_, token) => InvokeScheduledActionAsync(action, token), ct));
    }

    /// <summary>
    /// A named action: each occurrence is claimed in the schedule store before it runs, so brokers sharing the store run
    /// it once, and with <see cref="MissedRunPolicy.RunOnce"/> an occurrence missed while the broker was down runs on start.
    /// </summary>
    internal INymBroker AddNamedScheduledAction(string name, ScheduleTiming timing, MissedRunPolicy missedRuns, TimeSpan? maxCatchUp, Func<CancellationToken, Task> action)
    {
        ScheduledActionName.Validate(name);
        ArgumentNullException.ThrowIfNull(timing);
        ArgumentNullException.ThrowIfNull(action);

        _logger.LogInformation("Scheduled action {Name} ({Schedule}) set up, first occurrence at {NextOccurrence}, missed runs: {MissedRuns}",
            name, timing, timing.Next(DateTimeOffset.Now), missedRuns);
        return RegisterScheduledAction(ct => StartTimedScheduledAction(timing, (occurrence, token) => RunClaimedOccurrenceAsync(name, occurrence, action, token), ct,
            missedRuns == MissedRunPolicy.RunOnce ? token => RunMissedOccurrenceAsync(name, timing, maxCatchUp, action, token) : null));
    }

    /// <summary>
    /// Registers a scheduled action so every <see cref="StartAsync"/> starts it. If the scheduled actions are
    /// already running (the broker has started), the action is also started right away and tracked so
    /// <see cref="StopAsync"/> stops it (#50).
    /// </summary>
    /// <remarks>
    /// Guarded by <see cref="_scheduleLock"/> rather than the async lifecycle lock: starting an action is
    /// synchronous (it only queues a background loop), so the lock is never held across an await. That keeps
    /// <c>AddScheduledAction</c> non-blocking and safe to call from a consumer or another scheduled action
    /// while <c>StopAsync</c> is waiting for them.
    /// </remarks>
    private NymBrokerImpl RegisterScheduledAction(Func<CancellationToken, ScheduledActionHandle> start)
    {
        lock (_scheduleLock)
        {
            _scheduledActions = _scheduledActions.Add(start);

            if (_scheduledActionsRunning)
            {
                _activeScheduledActions = _activeScheduledActions.Add(start(CancellationToken.None));
                _logger.LogDebug("Broker already started — scheduled action started immediately");
            }
        }

        return this;
    }

    /// <summary>Starts every registered scheduled action. Called from <see cref="StartAsync"/>.</summary>
    private void StartScheduledActions(CancellationToken ct)
    {
        lock (_scheduleLock)
        {
            if (_scheduledActionsRunning)
                return;

            // Mark running first: if a start throws, the handles already started are still tracked
            // and StopScheduledActionsAsync (the caller's rollback) disposes them.
            _scheduledActionsRunning = true;
            foreach (var start in _scheduledActions)
                _activeScheduledActions = _activeScheduledActions.Add(start(ct));
        }
    }

    /// <summary>Stops every running scheduled action. Never throws because of an action (#51).</summary>
    private async Task StopScheduledActionsAsync()
    {
        ImmutableList<ScheduledActionHandle> handles;
        lock (_scheduleLock)
        {
            handles = _activeScheduledActions;
            _activeScheduledActions = ImmutableList<ScheduledActionHandle>.Empty;
            _scheduledActionsRunning = false;
        }

        foreach (var handle in handles)
            await handle.DisposeAsync();
    }

    private ScheduledActionHandle StartIntervalScheduledAction(TimeSpan initialDelay, TimeSpan interval, Func<CancellationToken, Task> action, CancellationToken ct)
    {
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = linkedCts.Token;
        var task = Task.Run(() => RunScheduledLoopAsync(async () =>
        {
            await Task.Delay(initialDelay, token);
            using var timer = new PeriodicTimer(interval);

            do
            {
                await InvokeScheduledActionAsync(action, token);
            }
            while (await timer.WaitForNextTickAsync(token));
        }, token), token);

        return new ScheduledActionHandle(linkedCts, task, _logger);
    }

    /// <summary>
    /// Runs <paramref name="runOccurrence"/> at each occurrence of <paramref name="timing"/>, after
    /// <paramref name="beforeFirst"/> (the catch-up of a missed run) if given.
    /// </summary>
    private ScheduledActionHandle StartTimedScheduledAction(
        ScheduleTiming timing,
        Func<DateTimeOffset, CancellationToken, Task> runOccurrence,
        CancellationToken ct,
        Func<CancellationToken, Task>? beforeFirst = null)
    {
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = linkedCts.Token;
        var task = Task.Run(() => RunScheduledLoopAsync(async () =>
        {
            if (beforeFirst is not null)
                await beforeFirst(token);

            DateTimeOffset? lastOccurrence = null;
            while (!token.IsCancellationRequested)
            {
                // Compute from the last occurrence when the timer woke up early, so one occurrence never runs twice.
                var now = DateTimeOffset.Now;
                var from = lastOccurrence > now ? lastOccurrence.Value : now;
                var nextOccurrence = timing.Next(from);
                if (nextOccurrence == null)
                    break;

                await DelayUntilAsync(nextOccurrence.Value, token);

                lastOccurrence = nextOccurrence;
                await runOccurrence(nextOccurrence.Value, token);
            }
        }, token), token);

        return new ScheduledActionHandle(linkedCts, task, _logger);
    }

    /// <summary>Waits until <paramref name="due"/>, in steps of at most <see cref="MaxScheduleDelay"/> so a far occurrence (e.g. <c>@yearly</c>) works.</summary>
    internal static async Task DelayUntilAsync(DateTimeOffset due, CancellationToken token)
    {
        TimeSpan delay;
        while ((delay = due - DateTimeOffset.Now) > TimeSpan.Zero)
            await Task.Delay(delay < MaxScheduleDelay ? delay : MaxScheduleDelay, token);
    }

    /// <summary>
    /// Start of a <see cref="MissedRunPolicy.RunOnce"/> action: if an occurrence was missed since the last recorded run,
    /// claim and run the latest one. A store failure is logged and the catch-up skipped; the schedule still starts.
    /// </summary>
    private async Task RunMissedOccurrenceAsync(string name, ScheduleTiming timing, TimeSpan? maxCatchUp, Func<CancellationToken, Task> action, CancellationToken token)
    {
        DateTimeOffset? lastRun;
        try
        {
            lastRun = await _scheduleStore.GetLastRunAsync(name, token);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !token.IsCancellationRequested)
        {
            _logger.LogError(ex, "Could not read the last run of scheduled action {Name}; a missed run is not caught up", name);
            return;
        }

        if (lastRun is null)
        {
            _logger.LogDebug("Scheduled action {Name} has no recorded run; nothing to catch up", name);
            return;
        }

        var missed = timing.FindMissed(lastRun.Value, DateTimeOffset.Now, maxCatchUp);
        if (missed is null)
            return;

        _logger.LogInformation("Scheduled action {Name} missed its run at {Occurrence} (last run {LastRun}); running it now",
            name, missed.Value, lastRun.Value);
        await RunClaimedOccurrenceAsync(name, missed.Value, action, token);
    }

    /// <summary>
    /// Claims <paramref name="occurrence"/> in the schedule store and runs the action if this instance got it. The claim
    /// counts as the run, whatever the action's outcome. A store failure is logged and the occurrence skipped.
    /// </summary>
    private async Task RunClaimedOccurrenceAsync(string name, DateTimeOffset occurrence, Func<CancellationToken, Task> action, CancellationToken token)
    {
        bool claimed;
        try
        {
            claimed = await _scheduleStore.TryClaimAsync(name, occurrence.ToUniversalTime(), token);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !token.IsCancellationRequested)
        {
            _logger.LogError(ex, "Could not claim the run at {Occurrence} of scheduled action {Name}; it is skipped", occurrence, name);
            return;
        }

        if (!claimed)
        {
            _logger.LogDebug("The run at {Occurrence} of scheduled action {Name} was already claimed, e.g. by another instance", occurrence, name);
            return;
        }

        await InvokeScheduledActionAsync(action, token, name);
    }

    /// <summary>
    /// Runs one occurrence; a failure is logged and the schedule continues (#51). Only cancellation of the
    /// schedule itself (broker stopping) propagates — an <see cref="OperationCanceledException"/> raised by the
    /// action for its own reasons (e.g. an HTTP timeout) is a failed run like any other.
    /// </summary>
    private async Task InvokeScheduledActionAsync(Func<CancellationToken, Task> action, CancellationToken token, string? name = null)
    {
        try
        {
            await action(token);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !token.IsCancellationRequested)
        {
            if (name is null)
                _logger.LogError(ex, "Scheduled action failed; it will run again at the next occurrence");
            else
                _logger.LogError(ex, "Scheduled action {Name} failed; it will run again at the next occurrence", name);
        }
    }

    /// <summary>Fire-and-forget boundary of a schedule loop: cancellation is a clean stop, anything else is logged.</summary>
    private async Task RunScheduledLoopAsync(Func<Task> loop, CancellationToken token)
    {
        try
        {
            await loop();
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Clean shutdown.
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Scheduled action loop terminated unexpectedly; the action will not run again until the broker is restarted");
        }
    }
}
