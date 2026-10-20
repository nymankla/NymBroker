---
name: nymbroker-scheduled
description: Run recurring work with NymBroker scheduled actions — interval timers and 5-field cron expressions — preferably by posting a message so a consumer does the work with retries and dead-lettering. Covers multi-instance pitfalls and cron syntax. Use when the user wants a timer, cron job, periodic poll, nightly/hourly job, heartbeat, recurring report or scheduled task in an app that uses NymBroker.
---

# NymBroker scheduled actions

`AddScheduledAction` on the `AddNymBroker()` builder runs an async action on an interval or a cron schedule while the broker is running. Each run gets its own DI scope and the broker's stop token. Actions start with `StartAsync` and stop with `StopAsync` (NymBroker 0.14+; earlier versions had synchronous `INymBroker.AddScheduledAction` overloads, which no longer exist).

Docs: https://github.com/nymankla/NymBroker/blob/master/docs/pipeline-extensions.md#scheduled-actions

## 1. Clarify

- **What** runs and **when** (every N seconds/minutes, or a calendar time like weekdays 17:00 — which time zone?).
- **How many instances** of the app run. Every instance runs every scheduled action. If the job must run once per occurrence across a cluster, see section 4.
- Does the work need retries? Then post a message and do the work in a consumer (recommended pattern below).
- Is NymBroker the right tool? For complex job scheduling (persistence of missed runs, dashboards, distributed locks) suggest a job scheduler (Quartz.NET, Hangfire) instead. Scheduled actions have no catch-up for runs missed while the app was down.

## 2. Recommended: schedule a message, work in a consumer

The action only posts a small "trigger" message; a consumer does the real work, so it gets retries, dead-lettering, metrics and a DI scope.

```csharp
using NymBroker.Core.Message;

[MessageName("reports.daily-report-requested")]
public sealed record DailyReportRequested(DateOnly Day);
```

```csharp
builder.Services.AddNymBroker()
    // ... endpoints, consumers ...

    // Weekdays at 17:00 local time.
    .AddScheduledAction("0 17 * * 1-5", (services, ct) =>
        services.GetRequiredService<INymBroker>()
            .PostAsync("Jobs", new DailyReportRequested(DateOnly.FromDateTime(DateTime.Now)), ct))

    // Every 5 minutes.
    .AddScheduledAction(TimeSpan.FromMinutes(5), (services, ct) =>
        services.GetRequiredService<INymBroker>().PostAsync("Jobs", new PollSupplierFeed(), ct))
    .Build();
```

Register a `Jobs` endpoint (a durable one if a trigger must not be lost) and a consumer for each trigger message (**nymbroker-consumer** skill).

## 3. API

| Builder method | Runs |
|---|---|
| `AddScheduledAction(TimeSpan interval, Func<IServiceProvider, CancellationToken, Task> action)` | every `interval` (must be positive); the first run is one interval after start |
| `AddScheduledAction(string cron, Func<IServiceProvider, CancellationToken, Task> action)` | on a cron schedule, local time zone |

- The `IServiceProvider` is a **new scope per run**, so scoped services (a `DbContext`) are safe to resolve from it. Don't capture services from outside the lambda.
- Each run is awaited before the next wait, so runs of one action never overlap. Pass `ct` to async calls; it is cancelled when the broker stops.
- A run that throws is logged at `Error`; the schedule continues. `StopAsync` never throws because of an action.
- An invalid cron expression throws `FormatException` when the action is added (at `AddScheduledAction`, not at startup).

### Cron syntax

Five fields: `minute hour day-of-month month day-of-week` (seconds are always 0), evaluated in the machine's **local time zone** — in containers that is usually UTC; set `TZ` or convert.

| Field | Values | Also |
|---|---|---|
| minute | 0-59 | `*` `,` `-` `/` |
| hour | 0-23 | `*` `,` `-` `/` |
| day-of-month | 1-31 | `?` `L` `L-n` `nW` `LW` |
| month | 1-12, `JAN`-`DEC` | |
| day-of-week | 0-7 (0 and 7 = Sunday), `SUN`-`SAT` | `?` `nL` (last weekday n) `n#k` (k-th weekday n) |

Examples: `*/15 * * * *` every 15 min · `0 2 * * *` daily 02:00 · `0 8 1 * *` 1st of the month 08:00 · `0 17 * * MON-FRI` weekdays 17:00 · `0 9 * * 1#1` first Monday 09:00 · macros `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly`. If both day-of-month and day-of-week are restricted, a day must match both. Daylight-saving gaps fire at the transition; fixed times in the fall-back hour fire once.

## 4. Several instances

Each instance fires every action. Options:

- Make the work idempotent and accept duplicates (e.g. "generate report for day X" that skips if it already exists). Usually the simplest.
- Run the schedule in one designated instance only (a config flag such as `Jobs:EnableScheduler`, checked before adding the actions).
- Use a dedicated scheduler for strict once-only semantics.

Tell the user which option you chose and why.

## 5. Finish

Build, run with a short interval or a near cron time to see one run (log line or consumed message), then restore the real schedule. Tell the user the schedule, the time zone it uses, and how it behaves with several instances.
