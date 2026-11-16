---
name: nymbroker-dashboard
description: Add the NymBroker dashboard (package NymBroker.Dashboard) to an ASP.NET Core app — a live page showing the message flow between endpoints, queue contents (pending, in progress, deferred, failed messages with their envelope, error and payload), endpoint health, throughput and latency. Covers AddNymBrokerDashboard / MapNymBrokerDashboard, securing it with an authorization policy, payload visibility and redaction, and its cost and limits. Use when the user wants a dashboard, admin UI, monitoring page, queue viewer, or to see failed or stuck messages in a NymBroker app.
---

# NymBroker dashboard

`NymBroker.Dashboard` serves a read-only live page from the app's own ASP.NET Core pipeline. It reads the broker in-process: no database, collector or extra service.

Docs: https://github.com/nymankla/NymBroker/blob/master/docs/dashboard.md

## 1. Check

- The app must be ASP.NET Core (`WebApplication`) on `net11.0`. For a worker service, either add a small web host or use metrics instead (see the `nymbroker-observability` skill).
- Find how the app authenticates operators (an existing policy such as `"Admin"`, Entra ID, cookie auth). Ask which policy should guard the dashboard; don't invent one.
- Ask whether operators may see message payloads, and whether payloads hold personal data.

## 2. Add it

```bash
dotnet add package NymBroker.Dashboard
```

```csharp
using NymBroker.Dashboard;

builder.Services.AddNymBroker() /* … endpoints, consumers … */ .Build();
builder.Services.AddNymBrokerDashboard(o =>
{
    o.Title = "Orders service";
    // o.ShowPayloads = true;                                   // default: only in Development
    // o.RedactPayload = (endpoint, bytes) => Mask(bytes);      // when payloads hold personal data
});

var app = builder.Build();
app.UseAuthentication();
app.UseAuthorization();
app.MapNymBrokerDashboard("/nymbroker").RequireAuthorization("Operations");
```

- Call `AddNymBrokerDashboard` once; `MapNymBrokerDashboard` throws if it is missing.
- Without `.RequireAuthorization(...)` the dashboard answers only requests from the local machine (a request through a reverse proxy counts as remote). That is fine for local development; in production always add a policy. `AllowAnonymousRemoteAccess = true` opens it to anyone — only on a closed network.
- The page is at `/nymbroker/` (with the trailing slash; `/nymbroker` redirects).

## 3. What the user gets

- Overview: messages/s, p99 processing time, oldest message age, failures; a flow map of endpoints, routes, topics, wire taps and the dead-letter endpoint; 15 minutes of traffic; queue depths.
- Endpoints and per-queue views: counts and paged lists per state, message details with envelope fields, dead-letter reason, last error and payload.
- Topology (routes, topics, consumers, scheduled actions with next run) and Health (with a "Check now" button).

Queue contents need a queue inspector: **SQLite, PostgreSQL, SQL Server and MySQL** endpoints have one. RabbitMQ, Service Bus, SQS, Memory, File and HTTP show health and rates only; point the user to the transport's own tools for those queues.

## 4. Options

`RefreshInterval` (2 s), `HealthInterval` (10 s), `History` (15 min, max 1 h), `ShowPayloads` (null = Development only), `MaxPayloadBytes` (64 KB), `RedactPayload`, `AllowAnonymousRemoteAccess`, `Title`.

## 5. Cost and limits — tell the user

- Metrics are collected in-process from startup with no allocation per message; in the benchmark the effect is within run-to-run noise.
- Queue counts run one query per inspectable endpoint per refresh, only while a page is open, shared by all open pages. Raise `RefreshInterval` for very large queue tables.
- Rates and latency are per process and in memory. With several instances each dashboard shows its own instance; for a cluster-wide view and alerting, export metrics with OpenTelemetry (`nymbroker-observability`).
- It is read-only: no replay or delete yet.

## 6. Verify

Run the app, open `/nymbroker/`, post a message, and check that it appears in the flow map's rates and (for a queue-table endpoint) in the counts. From another machine (or with an `X-Forwarded-For` header) the page must answer 403 unless the request is authorized.
