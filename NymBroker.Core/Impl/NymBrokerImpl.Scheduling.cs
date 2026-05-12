using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Impl;

public sealed partial class NymBrokerImpl
{
    public INymBroker AddScheduledAction(TimeSpan timeSpan, Action action)
    {
        ArgumentNullException.ThrowIfNull(action);
        _scheduledActions = _scheduledActions.Add(ct => StartIntervalScheduledActionAsync(timeSpan, timeSpan, action, ct));
        return this;
    }

    public INymBroker AddScheduledAction<T1>(TimeSpan timeSpan, Action<T1> action, T1 param1)
    {
        ArgumentNullException.ThrowIfNull(action);
        _scheduledActions = _scheduledActions.Add(ct => StartIntervalScheduledActionAsync(timeSpan, timeSpan, () => action(param1), ct));
        return this;
    }

    public INymBroker AddScheduledAction<T1, T2>(TimeSpan timeSpan, Action<T1, T2> action, T1 param1, T2 param2)
    {
        ArgumentNullException.ThrowIfNull(action);
        _scheduledActions = _scheduledActions.Add(ct => StartIntervalScheduledActionAsync(timeSpan, timeSpan, () => action(param1, param2), ct));
        return this;
    }

    public INymBroker AddScheduledAction<T1>(string expression, Action<T1> action, T1 param1)
    {
        ArgumentException.ThrowIfNullOrEmpty(expression);
        ArgumentNullException.ThrowIfNull(action);

        var cronAction = new CronScheduledAction<T1>(expression, action, param1);
        _logger.LogInformation("Scheduled cron action set up, first occurrence at {NextOccurrence}", cronAction.NextOccurrence(DateTimeOffset.Now));
        _scheduledActions = _scheduledActions.Add(ct => StartCronScheduledActionAsync(cronAction, ct));
        return this;
    }

    private Task<ScheduledActionHandle> StartIntervalScheduledActionAsync(TimeSpan initialDelay, TimeSpan interval, Action action, CancellationToken ct)
    {
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var task = Task.Run(async () =>
        {
            await Task.Delay(initialDelay, linkedCts.Token);
            using var timer = new PeriodicTimer(interval);

            do
            {
                action();
            }
            while (await timer.WaitForNextTickAsync(linkedCts.Token));
        }, linkedCts.Token);

        return Task.FromResult(new ScheduledActionHandle(linkedCts, task));
    }

    private Task<ScheduledActionHandle> StartCronScheduledActionAsync<T>(CronScheduledAction<T> cronAction, CancellationToken ct)
    {
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var task = Task.Run(async () =>
        {
            while (!linkedCts.Token.IsCancellationRequested)
            {
                var nextOccurrence = cronAction.NextOccurrence(DateTimeOffset.Now);
                if (nextOccurrence == null)
                    break;

                var delay = nextOccurrence.Value - DateTimeOffset.Now;
                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, linkedCts.Token);

                cronAction.Invoke();
            }
        }, linkedCts.Token);

        return Task.FromResult(new ScheduledActionHandle(linkedCts, task));
    }
}
