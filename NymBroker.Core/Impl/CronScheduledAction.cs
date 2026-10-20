using NymBroker.Core.Scheduling;

namespace NymBroker.Core.Impl;

internal sealed class CronScheduledAction(string expression, Func<CancellationToken, Task> action)
{
    private readonly CronSchedule _cron = CronSchedule.Parse(expression);

    public DateTimeOffset? NextOccurrence(DateTimeOffset from)
        => _cron.GetNextOccurrence(from, TimeZoneInfo.Local);

    public Task InvokeAsync(CancellationToken ct) => action(ct);
}
