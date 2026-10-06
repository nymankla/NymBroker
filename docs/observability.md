# Observability

[← User guide](user-guide.md)

NymBroker emits metrics and traces through the standard .NET APIs (`System.Diagnostics.Metrics` and `ActivitySource`), so any OpenTelemetry setup can collect them without an extra NymBroker package. It logs through `Microsoft.Extensions.Logging`.

## Enabling collection

```csharp
services.AddOpenTelemetry()
    .WithMetrics(m => m.AddMeter("NymBroker", "NymBroker.Resilience").AddOtlpExporter())
    .WithTracing(t => t.AddSource("NymBroker").AddOtlpExporter());
```

## Metrics

Meter `NymBroker`:

| Instrument | Type | Counts | Tags |
|---|---|---|---|
| `nymbroker.messages.received` | counter | messages entering processing | `source` |
| `nymbroker.messages.routed` | counter | copies forwarded to a destination, by a route or a topic | `source`, `destination`, `message_type`, `via` (`route`/`topic`), `topic`, `outcome` |
| `nymbroker.messages.consumed` | counter | consumer and subscriber calls | `source`, `message_type`, `consumer`, `kind` (`consumer`/`subscriber`), `outcome` |
| `nymbroker.messages.failed` | counter | processing failures | `source` |
| `nymbroker.messages.dead_lettered` | counter | dead-lettered messages | `reason`, `source`, `mode` (`broker`/`native`) |
| `nymbroker.message.processing.duration` | histogram (ms) | time to process one message | `source`, `outcome` (`success`/`failure`), `result` (`completed`/`retry`/`dead_letter`) |

Meter `NymBroker.Resilience`: `nymbroker.retries` — retry attempts by the File and RabbitMQ endpoints' retry policy.

Tag values are endpoint, topic, type and consumer names, so the number of series stays small.

Useful views:

- **Throughput and errors per endpoint** — `received` and `failed` by `source`.
- **Where messages go** — `routed` by `destination`, `consumed` by `consumer`.
- **Poison messages** — `dead_lettered` by `reason`; alert on any increase.
- **Latency** — `processing.duration` p95 by `source`; `result = retry` shows transient trouble.
- **Backlog** — queue depth isn't measured by the broker; take it from the transport (row count with `status = 0`, RabbitMQ / Service Bus metrics).

## Traces

Every processed message gets an activity `nymbroker.process` (kind *consumer*) from the source `NymBroker`, with these tags:

| Tag | Value |
|---|---|
| `messaging.system` | `nymbroker` |
| `nymbroker.source` | endpoint the message was received on |
| `messaging.message.id` | envelope `id` |
| `messaging.conversation_id` | envelope `correlationId` |
| `messaging.message.type` | envelope `messageType` |
| `error.type` | exception type, when processing failed |

Activities started inside consumers (HTTP calls, database commands) become children of it, so a trace shows everything one message caused.

## Logging

- While a message is processed, every log entry carries a scope with `MessageId`, `CorrelationId`, `MessageType` and `SourceEndpoint` — include scopes in your log output to correlate entries for one message.
- Failures are always logged: consumer failures and processing errors at `Error`, dead-lettering at `Warning` with its reason, listener loops that die at `Critical`. Nothing is swallowed silently.
- `AddMessageLoggingFilter()` logs every received message, including its payload, at `Debug`.
- Endpoint startup and shutdown are logged at `Information` ("Endpoint 'X' registered (ReadWrite)", "Started listening on endpoint 'X'").

## Health checks

`INymBroker.CheckHealthAsync()` checks the broker and every registered endpoint and returns one `BrokerHealthReport` (namespace `NymBroker.Core.Endpoint.HealthCheck`):

```csharp
BrokerHealthReport report = await broker.CheckHealthAsync(ct);

report.Status          // Healthy | Degraded | Unhealthy (aggregate)
report.BrokerStarted   // started and not stopped
report.Message         // what is wrong, e.g. "Unhealthy endpoints: Orders"; null when healthy
report.Duration
foreach (var ep in report.Endpoints)   // ordered by name
    Console.WriteLine($"{ep.Name} ({ep.Mode}): {ep.Status} {ep.Message} in {ep.Duration.TotalMilliseconds} ms");
```

- Each endpoint's `HealthCheck()` (connectivity to its database or broker, whether its listener is running) runs on the thread pool, all **in parallel**, so the call takes about as long as the slowest endpoint. The database and Service Bus endpoints contact their server with a 5-second timeout.
- An endpoint that has not answered within the overall **timeout** (default 10 s) is reported `Unhealthy` ("Health check timed out after …") and logged at `Warning`; its check finishes in the background.
- The call **never throws**: an endpoint whose `HealthCheck()` throws is reported `Unhealthy` with the exception message and logged at `Warning`. Only an `OperationCanceledException` from your own token propagates.

| Situation | Aggregate `Status` |
|---|---|
| All endpoints healthy and the broker started | `Healthy` |
| Only endpoints marked non-critical are unhealthy | `Degraded` |
| Any critical endpoint unhealthy | `Unhealthy` |
| Broker not started (or stopped) | `Unhealthy`, `BrokerStarted = false`, message "Broker is not running" |
| No endpoints registered | `Healthy` (only the broker state counts) |

All endpoints are critical by default. Mark the ones that shouldn't fail the whole broker, such as an audit sink or a wire tap, and change the timeout, with `ConfigureHealthCheck`:

```csharp
services.AddNymBroker()
    .AddFileEndPoint("Audit", new FileSettings { PostPath = "audit" }, EndpointMode.WriteOnly)
    .ConfigureHealthCheck(o =>
    {
        o.NonCritical("Audit");                 // unhealthy → Degraded, not Unhealthy
        o.Timeout = TimeSpan.FromSeconds(5);
    })
    .Build();
```

`Build()` throws if `NonCritical` names an endpoint that is not registered.

### ASP.NET Core and Kubernetes probes

`NymBrokerHealthCheck` adapts the report to `Microsoft.Extensions.Diagnostics.HealthChecks` (`using NymBroker.Core.DI;`):

```csharp
builder.Services.AddHealthChecks()
    .AddNymBroker(name: "nymbroker", tags: ["ready"]);

app.MapHealthChecks("/health/ready", new() { Predicate = r => r.Tags.Contains("ready") });
```

`Healthy` and `Degraded` map one to one; `Unhealthy` maps to the registration's `failureStatus` (`Unhealthy` unless you pass another). `HealthCheckResult.Description` is the report's message, and `Data` holds `"brokerStarted"` plus one entry per endpoint (`"Orders": "Unhealthy: <message>"`), so health UIs show which endpoint failed.

Health checks contact external systems, so don't run them on a hot path; a probe every few seconds is fine.
