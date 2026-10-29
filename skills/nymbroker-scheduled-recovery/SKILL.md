---
name: nymbroker-scheduled-recovery
description: Make NymBroker scheduled actions reliable with a schedule store — named actions that run once per occurrence across several app instances and catch up a run missed while the app was down (MissedRunPolicy.RunOnce), stored in SQLite, PostgreSQL, SQL Server or MySQL. Use when the user wants a scheduled/cron/nightly job to not be skipped after downtime, a restart or a deploy, to "recover missed runs", to run a scheduled job only once in a cluster / on one instance / with several replicas, or asks about NymBroker.Scheduling packages, AddScheduleStore or MissedRunPolicy.
---

# NymBroker named scheduled actions: run once, catch up missed runs

Plain scheduled actions (**nymbroker-scheduled** skill) run on every instance and forget a run that falls while the app is down. Giving an action a **name** makes the broker claim each occurrence in a **schedule store** before running it:

- instances sharing the store run each occurrence **once**;
- with `MissedRuns = MissedRunPolicy.RunOnce`, an action that missed one or more occurrences while the broker was down runs **once, right away** on start.

Requires a NymBroker version newer than 0.14.0 (the first with named actions and the `NymBroker.Scheduling.*` packages); check the installed version first. Docs: https://github.com/nymankla/NymBroker/blob/master/docs/pipeline-extensions.md#named-actions-run-once-across-instances-catch-up-missed-runs

## 1. Clarify

Ask only what the code and config don't already tell you:

- **Which job(s)** and their schedule. A job that must run "at least once per slot" (every missed hour must be processed separately) is **not** covered: `RunOnce` runs the latest missed occurrence once. For that, have the job process everything since its own last watermark (stored by the job), or use a dedicated scheduler.
- **How many instances** run, and on how many machines. That picks the store:

| Situation | Store | Package | Builder method |
|---|---|---|---|
| One process, recovery only after `StopAsync`/`StartAsync` in-process (rarely enough) | in memory (default) | `NymBroker` | — |
| One host (one or more processes on the same machine) | SQLite file | `NymBroker.Scheduling.Sqlite` | `AddSqliteScheduleStore` |
| Several machines / containers / replicas | the app's shared database | `NymBroker.Scheduling.Postgres` / `.SqlServer` / `.MySql` | `AddPostgresScheduleStore` / `AddSqlServerScheduleStore` / `AddMySqlScheduleStore` |

  Prefer the database the app already uses. Containers with ephemeral disks need a server database, not SQLite.
- **How old a missed run may be** and still be worth running (`MaxCatchUp`); e.g. a 15-minute price sync missed yesterday is pointless, a nightly report is not.
- Is the job safe to run when a previous run was **cut off half-way**? A claim counts as the run (see section 4).

## 2. Install and register the store

```bash
dotnet add package NymBroker.Scheduling.SqlServer   # or .Postgres / .MySql / .Sqlite — same version as NymBroker
```

```csharp
using NymBroker.Core.Scheduling;
using NymBroker.Scheduling.SqlServer;

builder.Services.AddNymBroker()
    // ... endpoints, consumers ...
    .AddSqlServerScheduleStore(new SqlServerScheduleStoreSettings
    {
        ConnectionString = builder.Configuration.GetConnectionString("Jobs")!,   // never rely on the default (local dev only)
        TableName = "dbo.nymbroker_schedule",                                   // created automatically
    })
```

| Settings class | Defaults |
|---|---|
| `SqliteScheduleStoreSettings` | `ConnectionString` required (e.g. `Data Source=schedule.db`), `TableName = "nymbroker_schedule"` |
| `PostgresScheduleStoreSettings` | `ConnectionString` required, `TableName = "nymbroker_schedule"` (may be `schema.table`) |
| `SqlServerScheduleStoreSettings` | `TableName = "dbo.nymbroker_schedule"`; the default connection string is the local dev container |
| `MySqlScheduleStoreSettings` | `TableName = "nymbroker_schedule"` (`db.table` allowed); the default connection string is the local dev container |

All have `AutoCreateTable = true`. In production with a least-privilege identity, create the table in a deployment step, set `AutoCreateTable = false` and grant `SELECT, INSERT, UPDATE` on it. The table has one row per action name and needs no cleanup.

Custom storage: implement `IScheduleStore` (`GetLastRunAsync(name)`, and an **atomic** `TryClaimAsync(name, occurrence)` that records the occurrence only if it is later than the stored one and returns whether it did), then `.AddScheduleStore(sp => new MyStore(...))`.

## 3. Name the actions

Add a name as the first argument and set the policy. Keep the recommended pattern of posting a trigger message so a consumer does the work with retries:

```csharp
    // Nightly at 02:00 local time; if the app was down at 02:00, run as soon as it is back.
    .AddScheduledAction("nightly-report", "0 2 * * *", (services, ct) =>
        services.GetRequiredService<INymBroker>().PostAsync("Jobs", new NightlyReportRequested(DateOnly.FromDateTime(DateTime.Now)), ct),
        o => o.MissedRuns = MissedRunPolicy.RunOnce)

    // Every 15 minutes, once per cluster; skip catch-up if the miss is older than an hour.
    .AddScheduledAction("sync-prices", TimeSpan.FromMinutes(15), (services, ct) =>
        services.GetRequiredService<INymBroker>().PostAsync("Jobs", new SyncPrices(), ct),
        o => { o.MissedRuns = MissedRunPolicy.RunOnce; o.MaxCatchUp = TimeSpan.FromHours(1); })

    // Once per cluster, no catch-up (the default policy is Skip).
    .AddScheduledAction("heartbeat", TimeSpan.FromMinutes(1), (services, ct) =>
        services.GetRequiredService<INymBroker>().PostAsync("Monitoring", new Heartbeat(Environment.MachineName), ct))
    .Build();
```

If the job posts a trigger message to a shared durable queue, the queue spreads the work; the store makes sure only one trigger is posted per occurrence.

## 4. Rules to apply and to tell the user

- **Names are keys**: unique in the app (case-insensitive, duplicates throw), at most 200 characters, and stable. Renaming an action loses its history, so the first start after a rename catches nothing up. Apps sharing a table must not reuse a name for different jobs. Use a `const string` per job.
- **A claim counts as the run.** The occurrence is recorded *before* the action runs. A run that throws, or that is killed by a deploy, is not retried at the next start. If that matters, make the job resumable or idempotent, or post a trigger message to a durable queue (the queue retries).
- **Named interval actions run on a fixed grid**: multiples of the interval since 1970-01-01 UTC (`FromMinutes(15)` → :00, :15, :30, :45; `FromHours(1)` → on the hour UTC). The first run is at the next grid point, not one interval after start. The interval must be whole milliseconds. For a specific time of day, use cron.
- **Cron** uses the machine's local time zone. All instances sharing the store must use the same time zone; in containers that is usually UTC unless `TZ` is set.
- **Catch-up needs history**: the first time an action ever starts there is no recorded run, so nothing is caught up. `RunOnce` runs **one** run for any number of missed occurrences.
- **Store down**: claiming an occurrence fails → it is skipped (logged at `Error`, never run twice). Reading the last run fails at start → no catch-up that time. Alert on these log errors (**nymbroker-observability** skill).
- Unnamed `AddScheduledAction(interval | cron, action)` keeps the old behaviour: every instance, no store.

## 5. Test

- Unit test: register a named action with a short interval (`TimeSpan.FromMilliseconds(200)`) and the default in-memory store, or an `IScheduleStore` fake via `.AddScheduleStore(_ => fake)`. Assert runs by counting, as in the **nymbroker-testing** skill.
- Catch-up: before building the broker, call `await store.TryClaimAsync("nightly-report", DateTimeOffset.UtcNow.AddDays(-2))` on a store over the same table/file, then start the broker with `RunOnce` and assert the action runs within a few seconds.
- Locally against a real database: run the app, stop it across an occurrence (or set the row's `last_run_utc` back), start it, and look for the `Information` log `Scheduled action {Name} missed its run at …; running it now`.

## 6. Finish

Build with 0 warnings and run the tests. Tell the user which store and table you chose and why, each action's name, schedule, time zone and policy, and the trade-off that a claim counts as the run.
