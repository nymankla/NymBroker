---
name: nymbroker-scheduled
description: Run recurring work with NymBroker scheduled actions — interval timers and 5-field cron expressions — preferably by posting a message so a consumer does the work with retries and dead-lettering. Covers multi-instance pitfalls and cron syntax. Use when the user wants a timer, cron job, periodic poll, nightly/hourly job, heartbeat, recurring report or scheduled task in an app that uses NymBroker.
---

# NymBroker scheduled actions

`INymBroker.AddScheduledAction` runs a synchronous `Action` on an interval or a cron schedule while the broker is running. Actions start with `StartAsync`, stop with `StopAsync`, and can be added before or after start.

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
// After the app/host is built, before it runs:
var broker = app.Services.GetRequiredService<INymBroker>();

// Weekdays at 17:00 local time.
broker.AddScheduledAction<INymBroker>(
    "0 17 * * 1-5",
    b => b.PostAsync("Jobs", new DailyReportRequested(DateOnly.FromDateTime(DateTime.Now))).GetAwaiter().GetResult(),
    broker);

// Every 5 minutes.
broker.AddScheduledAction<INymBroker>(
    TimeSpan.FromMinutes(5),
    b => b.PostAsync("Jobs", new PollSupplierFeed()).GetAwaiter().GetResult(),
    broker);
```

Register a `Jobs` endpoint (a durable one if a trigger must not be lost) and a consumer for each trigger message (**nymbroker-consumer** skill).

## 3. API

| Overload | Runs |
|---|---|
| `AddScheduledAction(TimeSpan interval, Action action)` | every `interval`; the first run is one interval after start |
| `AddScheduledAction<T1>(TimeSpan, Action<T1>, T1 arg)` / `<T1, T2>(TimeSpan, Action<T1, T2>, T1, T2)` | same, with arguments |
| `AddScheduledAction<T1>(string cron, Action<T1>, T1 arg)` | on a cron schedule, local time zone |

- Actions are **synchronous**. For async code use `.GetAwaiter().GetResult()` inside the action (it runs on its own background task), or better, post a message.
- Don't capture scoped services (e.g. a `DbContext`) in the action — it lives for the app's lifetime. Create a scope inside (`using var scope = app.Services.CreateScope();`) or use the message pattern.
- A run that throws is logged at `Error`; the schedule continues. `StopAsync` never throws because of an action.
- An invalid cron expression throws `FormatException` when the action is added.

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
