using Cronos;

namespace MessageBroker.Core.Impl;

internal sealed class CronScheduledAction<T>(string expression, Action<T> action, T parameter)
{
    private readonly CronExpression _cron = CronExpression.Parse(expression);

    public DateTimeOffset? NextOccurrence(DateTimeOffset from)
        => _cron.GetNextOccurrence(from, TimeZoneInfo.Local);

    public void Invoke() => action(parameter);
}
