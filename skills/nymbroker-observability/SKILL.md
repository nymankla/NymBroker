---
name: nymbroker-observability
description: Add monitoring to a NymBroker application — OpenTelemetry metrics and traces (meters "NymBroker" and "NymBroker.Resilience", activity source "NymBroker"), ASP.NET Core / Kubernetes health checks with non-critical endpoints, structured logging scopes, and recommended alerts and dashboards. Use when the user wants metrics, tracing, telemetry, OpenTelemetry, Application Insights, Prometheus, Grafana, health checks, readiness/liveness probes, alerts or dashboards for NymBroker.
---

# NymBroker observability

NymBroker uses the standard .NET APIs (`System.Diagnostics.Metrics`, `ActivitySource`, `ILogger`), so any OpenTelemetry setup collects it — there is no NymBroker telemetry package.

Docs: https://github.com/nymankla/NymBroker/blob/master/docs/observability.md

## 1. Ask

Find out (from the code, or ask): where telemetry goes (OTLP collector, Azure Monitor / Application Insights, Prometheus, console for now), whether OpenTelemetry is already configured (`AddOpenTelemetry()` — extend it, don't add a second one), whether the app is ASP.NET Core (health endpoint) or a worker, and which endpoints are optional (audit, wire tap) so they shouldn't fail health.

## 2. Metrics and traces

```bash
dotnet add package OpenTelemetry.Extensions.Hosting
dotnet add package OpenTelemetry.Exporter.OpenTelemetryProtocol    # or Azure.Monitor.OpenTelemetry.AspNetCore, OpenTelemetry.Exporter.Prometheus.AspNetCore, …
```

```csharp
builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("orders-worker"))
    .WithMetrics(m => m
        .AddMeter("NymBroker", "NymBroker.Resilience")
        .AddOtlpExporter())
    .WithTracing(t => t
        .AddSource("NymBroker")
        .AddOtlpExporter());
```

Add the instrumentations the app already uses (ASP.NET Core, HttpClient, SQL client) so a message's trace shows its database and HTTP calls. With a listener sampling `NymBroker`, processing can create `nymbroker.process` (kind consumer); typed posts, batches, route/topic forwards and broker dead-letter copies can create `nymbroker.send` (kind producer). The broker can carry W3C context (`traceParent` / `traceState`) in its envelopes across transports. Raw posts, wire-tap copies and outbox writes do not start send activities. Options: `.ConfigureTracing(o => o.Continuation = TraceContinuation.Link)` for a new trace linked to the producer; `o.PropagateTraceContext = false` stops stamping or forwarding context in broker-serialized envelopes (namespace `NymBroker.Core.Diagnostics`). It does not sanitize raw posts or wire-tap copies: inspect and sanitize those separately before crossing a trust boundary. Details: `docs/observability.md#trace-context-across-messages`.

Key instruments (meter `NymBroker`):

| Instrument | Use it for |
|---|---|
| `nymbroker.messages.received` (by `source`) | throughput per endpoint |
| `nymbroker.messages.failed` (by `source`) | error rate |
| `nymbroker.messages.dead_lettered` (by `reason`, `source`, `mode`) | poison messages — alert on any increase |
| `nymbroker.message.processing.duration` (ms; `source`, `outcome`, `result`) | latency p95; `result = retry` shows transient trouble |
| `nymbroker.message.age` (s; `source`) | time from the envelope's `created` to processing; rises when a backlog builds (sender clock skew shifts it) |
| `nymbroker.messages.consumed` (`consumer`, `kind`, `outcome`) | per-consumer volume and failures |
| `nymbroker.messages.routed` (`destination`, `via`) | where routed/topic copies go |
| `nymbroker.messages.duplicates` | duplicates dropped by the idempotent receiver |
| `nymbroker.health.status` (gauge 0/1/2), `nymbroker.health.endpoint.failures` | health over time (recorded when a health check runs) |
| `nymbroker.retries` (meter `NymBroker.Resilience`) | RetryPolicy attempts, including transport, HTTP and broker-held deferred sends |

For SQL queues, `broker.GetQueueInspector("Orders")?.GetCountsAsync(ct)` exposes counts when the endpoint supports inspection. For other transports use their own metrics (for example Service Bus or RabbitMQ).

## 3. Health checks (ASP.NET Core)

```csharp
using NymBroker.Core.DI;

builder.Services.AddNymBroker()
    // … endpoints …
    .ConfigureHealthCheck(o =>
    {
        o.NonCritical("Audit");                // unhealthy → Degraded instead of Unhealthy
        o.Timeout = TimeSpan.FromSeconds(5);   // default 10 s
    })
    .Build();

builder.Services.AddHealthChecks().AddNymBroker(name: "nymbroker", tags: ["ready"]);

app.MapHealthChecks("/health/ready", new() { Predicate = r => r.Tags.Contains("ready") });
app.MapHealthChecks("/health/live", new() { Predicate = _ => false });   // liveness: process up, no dependency checks
```

- The check is `Unhealthy` when the broker isn't started or any critical endpoint can't reach its server; `Degraded` when only non-critical ones fail. `Data` lists each endpoint's status.
- Use it for **readiness**, not liveness — a database outage shouldn't restart every pod.
- `Build()` throws if `NonCritical` names an unknown endpoint.
- In a worker without ASP.NET Core, call `await broker.CheckHealthAsync(ct)` (returns `BrokerHealthReport`; caller cancellation propagates) from your own probe or a timer, or add a minimal health endpoint.
- Checks contact external systems; probe every few seconds at most.

Queue inspection is available for SQLite, PostgreSQL, SQL Server and MySQL endpoints; `GetQueueInspector` returns `null` for the others. See `docs/observability.md#queue-inspection` for counts and paging.

## 4. Logging

- Each message's log entries carry a scope with `MessageId`, `CorrelationId`, `MessageType`, `SourceEndpoint`. Enable scopes in the log provider (console: `"Logging": { "Console": { "IncludeScopes": true } }`; OpenTelemetry logging: `IncludeScopes = true`).
- Failures are logged at `Error`, dead-lettering at `Warning`, a dead listener loop at `Critical` — alert on `Critical` from category `NymBroker*`.
- `AddMessageLoggingFilter()` logs every message with payload at `Debug` — for debugging only (payloads may contain personal data).

## 5. Alerts and dashboard

Suggest to the user:

- Alert: `dead_lettered` increase > 0; `failed` rate above normal; `health.status > 0` for several minutes; any `Critical` log from NymBroker; backlog (transport metric) growing for N minutes.
- Dashboard: received and failed per `source`; processing duration p50/p95 per `source`; dead letters by `reason`; consumed per `consumer`; health status; backlog (`message.age` p95/max per `source`, depth from the transport).

## 6. Finish

Build, run, post a message, and show where it appears (trace, metric, health endpoint output). List the alerts the user still has to create in their monitoring tool.
