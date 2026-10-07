using NymBroker.Core.Scheduling;

namespace NymBroker.Core.Impl;

internal sealed class CronScheduledAction<T>(string expression, Action<T> action, T parameter)
{
    private readonly CronSchedule _cron = CronSchedule.Parse(expression);

    public DateTimeOffset? NextOccurrence(DateTimeOffset from)
        => _cron.GetNextOccurrence(from, TimeZoneInfo.Local);

    public void Invoke() => action(parameter);
}
