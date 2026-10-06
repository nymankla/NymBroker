using System.Collections.Immutable;
using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Impl;

public sealed partial class NymBrokerImpl
{
    public INymBroker AddScheduledAction(TimeSpan timeSpan, Action action)
    {
        ArgumentNullException.ThrowIfNull(action);
        return RegisterScheduledAction(ct => StartIntervalScheduledAction(timeSpan, timeSpan, action, ct));
    }

    public INymBroker AddScheduledAction<T1>(TimeSpan timeSpan, Action<T1> action, T1 param1)
    {
        ArgumentNullException.ThrowIfNull(action);
        return RegisterScheduledAction(ct => StartIntervalScheduledAction(timeSpan, timeSpan, () => action(param1), ct));
    }

    public INymBroker AddScheduledAction<T1, T2>(TimeSpan timeSpan, Action<T1, T2> action, T1 param1, T2 param2)
    {
        ArgumentNullException.ThrowIfNull(action);
        return RegisterScheduledAction(ct => StartIntervalScheduledAction(timeSpan, timeSpan, () => action(param1, param2), ct));
    }

    public INymBroker AddScheduledAction<T1>(string expression, Action<T1> action, T1 param1)
    {
        ArgumentException.ThrowIfNullOrEmpty(expression);
        ArgumentNullException.ThrowIfNull(action);

        var cronAction = new CronScheduledAction<T1>(expression, action, param1);
        _logger.LogInformation("Scheduled cron action set up, first occurrence at {NextOccurrence}", cronAction.NextOccurrence(DateTimeOffset.Now));
        return RegisterScheduledAction(ct => StartCronScheduledAction(cronAction, ct));
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

    private ScheduledActionHandle StartIntervalScheduledAction(TimeSpan initialDelay, TimeSpan interval, Action action, CancellationToken ct)
    {
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = linkedCts.Token;
        var task = Task.Run(() => RunScheduledLoopAsync(async () =>
        {
            await Task.Delay(initialDelay, token);
            using var timer = new PeriodicTimer(interval);

            do
            {
                InvokeScheduledAction(action, token);
            }
            while (await timer.WaitForNextTickAsync(token));
        }, token), token);

        return new ScheduledActionHandle(linkedCts, task, _logger);
    }

    private ScheduledActionHandle StartCronScheduledAction<T>(CronScheduledAction<T> cronAction, CancellationToken ct)
    {
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = linkedCts.Token;
        var task = Task.Run(() => RunScheduledLoopAsync(async () =>
        {
            DateTimeOffset? lastOccurrence = null;
            while (!token.IsCancellationRequested)
            {
                // Compute from the last occurrence when the timer woke up early, so one occurrence never runs twice.
                var now = DateTimeOffset.Now;
                var from = lastOccurrence > now ? lastOccurrence.Value : now;
                var nextOccurrence = cronAction.NextOccurrence(from);
                if (nextOccurrence == null)
                    break;

                var delay = nextOccurrence.Value - DateTimeOffset.Now;
                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, token);

                lastOccurrence = nextOccurrence;
                InvokeScheduledAction(cronAction.Invoke, token);
            }
        }, token), token);

        return new ScheduledActionHandle(linkedCts, task, _logger);
    }

    /// <summary>
    /// Runs one occurrence; a failure is logged and the schedule continues (#51). Only cancellation of the
    /// schedule itself (broker stopping) propagates — an <see cref="OperationCanceledException"/> raised by the
    /// action for its own reasons (e.g. an HTTP timeout) is a failed run like any other.
    /// </summary>
    private void InvokeScheduledAction(Action action, CancellationToken token)
    {
        try
        {
            action();
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !token.IsCancellationRequested)
        {
            _logger.LogError(ex, "Scheduled action failed; it will run again at the next occurrence");
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
