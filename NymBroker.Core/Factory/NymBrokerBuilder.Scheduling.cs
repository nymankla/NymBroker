using NymBroker.Core.Scheduling;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace NymBroker.Core.Factory;

public sealed partial class NymBrokerBuilder
{
    /// <summary>
    /// Run <paramref name="action"/> every <paramref name="interval"/> while the broker runs, first after one interval.
    /// Each run gets its own DI scope; a failing run is logged and the schedule continues.
    /// </summary>
    public NymBrokerBuilder AddScheduledAction(TimeSpan interval, Func<IServiceProvider, CancellationToken, Task> action)
    {
        if (interval <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(interval), "The interval must be positive.");
        ArgumentNullException.ThrowIfNull(action);
        _scheduledActions.Add((broker, sp) => broker.AddScheduledAction(interval, ct => RunScopedAsync(sp, action, ct)));
        return this;
    }

    /// <summary>
    /// Run <paramref name="action"/> on a cron schedule (five fields or a macro such as <c>@daily</c>, local time zone) while the broker runs.
    /// Each run gets its own DI scope; a failing run is logged and the schedule continues.
    /// </summary>
    public NymBrokerBuilder AddScheduledAction(string cronExpression, Func<IServiceProvider, CancellationToken, Task> action)
    {
        ArgumentException.ThrowIfNullOrEmpty(cronExpression);
        ArgumentNullException.ThrowIfNull(action);
        _ = CronSchedule.Parse(cronExpression);   // an invalid expression throws here, not at startup
        _scheduledActions.Add((broker, sp) => broker.AddScheduledAction(cronExpression, ct => RunScopedAsync(sp, action, ct)));
        return this;
    }

    // RS0026: the two named overloads differ in a required parameter (TimeSpan interval / string cron), so calls are never ambiguous.
#pragma warning disable RS0026
    /// <summary>
    /// Run a named action every <paramref name="interval"/> while the broker runs. Occurrences are on a fixed grid
    /// (multiples of the interval since 1970-01-01 UTC) and each is claimed in the schedule store before it runs:
    /// brokers sharing a store (<see cref="AddScheduleStore"/>) run it once per occurrence, and
    /// <see cref="ScheduledActionOptions.MissedRuns"/> can catch up a run missed while the broker was down.
    /// </summary>
    /// <param name="name">Unique (case-insensitive) key of the action in the schedule store; at most 200 characters.</param>
    /// <param name="interval">Positive, whole milliseconds.</param>
    public NymBrokerBuilder AddScheduledAction(string name, TimeSpan interval, Func<IServiceProvider, CancellationToken, Task> action, Action<ScheduledActionOptions>? configure = null)
        => AddNamedScheduledAction(name, new GridTiming(interval), action, configure);

    /// <summary>
    /// Run a named action on a cron schedule (local time zone) while the broker runs. Each occurrence is claimed in the
    /// schedule store before it runs: brokers sharing a store (<see cref="AddScheduleStore"/>) run it once per
    /// occurrence, and <see cref="ScheduledActionOptions.MissedRuns"/> can catch up a run missed while the broker was down.
    /// </summary>
    /// <param name="name">Unique (case-insensitive) key of the action in the schedule store; at most 200 characters.</param>
    public NymBrokerBuilder AddScheduledAction(string name, string cronExpression, Func<IServiceProvider, CancellationToken, Task> action, Action<ScheduledActionOptions>? configure = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(cronExpression);
        return AddNamedScheduledAction(name, new CronTiming(CronSchedule.Parse(cronExpression)), action, configure);
    }
#pragma warning restore RS0026

    private NymBrokerBuilder AddNamedScheduledAction(string name, ScheduleTiming timing, Func<IServiceProvider, CancellationToken, Task> action, Action<ScheduledActionOptions>? configure)
    {
        ScheduledActionName.Validate(name);
        ArgumentNullException.ThrowIfNull(action);
        var options = new ScheduledActionOptions();
        configure?.Invoke(options);
        options.Validate();
        if (!_scheduledActionNames.Add(name))
            throw new ArgumentException($"A scheduled action named '{name}' is already added.", nameof(name));

        var (missedRuns, maxCatchUp) = (options.MissedRuns, options.MaxCatchUp);
        _scheduledActions.Add((broker, sp) => broker.AddNamedScheduledAction(name, timing, missedRuns, maxCatchUp, ct => RunScopedAsync(sp, action, ct)));
        return this;
    }

    /// <summary>
    /// Where named scheduled actions record their runs: a store shared by several instances makes each occurrence run
    /// once, and a durable one lets <see cref="MissedRunPolicy.RunOnce"/> catch up across restarts. The
    /// <c>NymBroker.Scheduling.*</c> packages call this. Without it, an in-process store is used. The last registration wins.
    /// </summary>
    public NymBrokerBuilder AddScheduleStore(Func<IServiceProvider, IScheduleStore> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        _services.RemoveAll<IScheduleStore>();
        _services.AddSingleton(factory);
        _scheduleStore = true;
        return this;
    }

    private static async Task RunScopedAsync(IServiceProvider services, Func<IServiceProvider, CancellationToken, Task> action, CancellationToken ct)
    {
        await using var scope = services.CreateAsyncScope();
        await action(scope.ServiceProvider, ct);
    }

    // --- Dead letter, wire tap, TTL, idempotency ---
}
