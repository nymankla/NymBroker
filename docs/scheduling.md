# Scheduling

[← User guide](user-guide.md)

Run code on an interval or a cron schedule while the broker runs: post a heartbeat, poll an external system, or trigger a nightly job. Plain scheduled actions run on every instance. **Named** actions claim each occurrence in a schedule store, so they run once across instances and can catch up a run missed while the app was down.

- [Scheduled actions](#scheduled-actions) — intervals, cron, DI scopes, errors
- [Cron syntax](#cron-syntax)
- [Named actions: run once across instances, catch up missed runs](#named-actions-run-once-across-instances-catch-up-missed-runs) — schedule stores, `MissedRunPolicy`, behaviour

## Scheduled actions

Scheduled actions are added on the builder. Each run gets its own DI scope (`services`) and the broker's stop token:

```csharp
services.AddNymBroker()
    // ...
    // Every minute (first run one minute after start).
    .AddScheduledAction(TimeSpan.FromMinutes(1), (services, ct) =>
        services.GetRequiredService<INymBroker>().PostAsync("Prices", new PriceRequest("ACME"), ct))

    // Cron (5-field syntax, local time zone): weekdays at 17:00.
    .AddScheduledAction("0 17 * * 1-5", async (services, ct) =>
        await services.GetRequiredService<ReportJob>().RunAsync(ct))   // scoped services work, e.g. a DbContext
    .Build();
```

### Cron syntax

Five fields, `minute hour day-of-month month day-of-week`, separated by whitespace; seconds are always 0. Expressions are evaluated in the machine's local time zone. The parser is built in and is compatible with Cronos `CronFormat.Standard`, which was used before.

| Field | Values | Also allowed |
|---|---|---|
| minute | 0-59 | `*` `,` `-` `/` |
| hour | 0-23 | `*` `,` `-` `/` |
| day-of-month | 1-31 | `*` `?` `,` `-` `/` `L` `L-n` `nW` `LW` |
| month | 1-12 or `JAN`-`DEC` | `*` `,` `-` `/` |
| day-of-week | 0-7 (0 and 7 = Sunday) or `SUN`-`SAT` | `*` `?` `,` `-` `/` `nL` (last weekday n of the month) `n#k` (k-th weekday n) |

- Steps: `*/s`, `a-b/s` and `a/s` (from `a` to the field's maximum). Names are case-insensitive. Ranges must not be reversed (`5-1` is invalid).
- Macros: `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, `@hourly`.
- When both day-of-month and day-of-week are restricted, a day must match both.
- An expression with no future occurrence (such as `0 0 30 2 *`) never fires.
- Daylight saving time: an occurrence in the spring-forward gap fires at the transition; in the fall-back overlap, fixed-time expressions fire once (first pass) and interval-style expressions (`*`, ranges or steps in the minute or hour field, e.g. `*/15 * * * *`) fire in both offsets.
- An invalid expression throws a `FormatException` when the action is added.

- Actions are asynchronous: `(IServiceProvider services, CancellationToken ct) => Task`. A run is awaited before the schedule waits for the next one, so runs of one action never overlap. Pass `ct` on; it is cancelled when the broker stops.
- Actions start with `StartAsync`, stop with `StopAsync`, and start again with a later `StartAsync`. They can't be added after the broker is built.
- An interval must be positive; it throws `ArgumentOutOfRangeException` otherwise.
- A cron action whose next occurrence is months away (`@yearly`) waits for it (up to 0.14.0, its loop stopped with an error when the wait was over about 49 days).
- A run that throws is logged at `Error` and the schedule continues with the next occurrence. `StopAsync` never throws because of an action.

## Named actions: run once across instances, catch up missed runs

The actions above run on every instance of the app and forget a run that falls while the app is down. Give an action a **name** to change that. Before it runs an occurrence, the broker claims it in a **schedule store**, so:

- instances that share the store run each occurrence **once**, on whichever instance claims it first;
- with `MissedRuns = MissedRunPolicy.RunOnce`, an action that missed one or more occurrences while the broker was not running runs **once, right away** on start, then follows its schedule.

```csharp
// dotnet add package NymBroker.Scheduling.SqlServer
using NymBroker.Core.Scheduling;
using NymBroker.Scheduling.SqlServer;

services.AddNymBroker()
    // ...
    .AddSqlServerScheduleStore(new SqlServerScheduleStoreSettings
    {
        ConnectionString = builder.Configuration.GetConnectionString("Jobs")!,
        TableName = "dbo.nymbroker_schedule"
    })

    // Nightly at 02:00; if the app was down at 02:00, run as soon as it starts again.
    .AddScheduledAction("nightly-report", "0 2 * * *", (services, ct) =>
        services.GetRequiredService<INymBroker>().PostAsync("Jobs", new NightlyReportRequested(), ct),
        o => o.MissedRuns = MissedRunPolicy.RunOnce)

    // Every 15 minutes, once per cluster; a run missed more than an hour ago is not caught up.
    .AddScheduledAction("sync-prices", TimeSpan.FromMinutes(15), (services, ct) =>
        services.GetRequiredService<PriceSync>().RunAsync(ct),
        o => { o.MissedRuns = MissedRunPolicy.RunOnce; o.MaxCatchUp = TimeSpan.FromHours(1); })
    .Build();
```

| Store | Package | Builder method | Scope |
|---|---|---|---|
| In memory (default) | `NymBroker` | — | One process: no recovery after a restart; catch-up only after `StopAsync` / `StartAsync` |
| SQLite | `NymBroker.Scheduling.Sqlite` | `AddSqliteScheduleStore` | One host: survives restarts; processes on the machine may share the file |
| PostgreSQL | `NymBroker.Scheduling.Postgres` | `AddPostgresScheduleStore` | Shared by every instance using the table |
| SQL Server | `NymBroker.Scheduling.SqlServer` | `AddSqlServerScheduleStore` | Shared by every instance using the table |
| MySQL 8.0+ / MariaDB 10.6+ | `NymBroker.Scheduling.MySql` | `AddMySqlScheduleStore` | Shared by every instance using the table |

Each store settings class has `ConnectionString`, `TableName` (default `nymbroker_schedule`, `dbo.nymbroker_schedule` for SQL Server) and `AutoCreateTable` (default `true`). The table holds one row per action name with the last claimed occurrence, so it needs no cleanup. To use a different storage, implement `IScheduleStore` (`GetLastRunAsync`, an atomic `TryClaimAsync`) and register it with `.AddScheduleStore(sp => ...)`.

How named actions behave:

- **The name** is the key in the store: unique per app (case-insensitive; adding a name twice throws `ArgumentException`), at most 200 characters, and stable. Renaming an action loses its history. Apps that share a table must not reuse a name for different work.
- **A claim counts as the run.** The occurrence is recorded before the action runs, so a run that throws, or that the app is stopped in the middle of, is not repeated. Make long jobs resumable or idempotent if that matters.
- **Interval actions run on a fixed grid**: multiples of the interval since 1970-01-01 UTC, e.g. `TimeSpan.FromMinutes(15)` runs at :00, :15, :30 and :45. That is how instances started at different times agree on an occurrence. (Unnamed interval actions still run one interval after start.) The interval must be a whole number of milliseconds.
- **Cron actions** use the machine's local time zone. Instances that share a store must use the same time zone.
- **Catching up:** `RunOnce` runs the latest missed occurrence once (not one run per missed occurrence). It needs a recorded run, so nothing is caught up the first time an action ever starts. `MaxCatchUp` limits how old that occurrence may be; the default `null` means any age.
- **Store failures** are logged at `Error`. If a claim fails (the database is down), that occurrence is skipped and not run, so it is never run twice. If reading the last run fails at start, nothing is caught up. The schedule keeps going either way.
- Several instances: an instance that loses a claim logs it at `Debug` and waits for the next occurrence. The instances' clocks only need to agree to within the interval; the store's own clock is not used.
