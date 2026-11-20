# Observability

[← User guide](user-guide.md)

NymBroker emits metrics and traces through the standard .NET APIs (`System.Diagnostics.Metrics` and `ActivitySource`), so any OpenTelemetry setup can collect them without an extra NymBroker package. It logs through `Microsoft.Extensions.Logging`.

To look at a running broker without setting any of that up, add the [dashboard](dashboard.md): a page in your app that shows the message flow, queue contents, health and throughput from these same metrics. `INymBroker.Describe()` returns the broker's configuration (endpoints and their capabilities, routes, topics, consumers, scheduled actions with their next run) for your own tools.

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
| `nymbroker.messages.replayed` | counter | failed queue messages moved back to pending through [`GetQueueReplayer`](reliability.md#inspecting-and-replaying) | `endpoint` |
| `nymbroker.messages.duplicates` | counter | duplicates dropped by the [idempotent receiver](reliability.md#duplicate-detection) | `source` |
| `nymbroker.message.processing.duration` | histogram (ms) | time to process one message | `source`, `outcome` (`success`/`failure`), `result` (`completed`/`retry`/`dead_letter`/`cancelled`) |
| `nymbroker.message.age` | histogram (s) | time from the envelope's `created` to the start of processing; not recorded when `created` is missing, never negative | `source` |
| `nymbroker.health.checks` | counter | [health checks](#health-checks) run | `status` (`healthy`/`degraded`/`unhealthy`) |
| `nymbroker.health.endpoint.failures` | counter | endpoints reported unhealthy by a health check | `endpoint`, `critical` (`true`/`false`), `reason` (`unhealthy`/`timeout`/`error`) |
| `nymbroker.health.status` | gauge | aggregate status of the latest health check: `0` healthy, `1` degraded, `2` unhealthy | — |
| `nymbroker.health.endpoint.healthy` | gauge | health of each endpoint in the latest check: `1` reachable (healthy, or degraded by a dead-letter threshold), `0` unhealthy | `endpoint`, `critical` |
| `nymbroker.queue.messages` | gauge | messages in a queue endpoint, from the latest [queue monitoring](#queue-monitoring) sample (only with `MonitorQueues`) | `endpoint`, `state` (`pending`/`in_progress`/`deferred`/`failed`) |
| `nymbroker.queue.oldest_message.age` | gauge (s) | age of the oldest pending message (since posted) and failed message (since it failed), from the latest sample | `endpoint`, `state` (`pending`/`failed`) |

Meter `NymBroker.Resilience` (it keeps its original name from when `RetryPolicy` was a separate package):

| Instrument | Type | What it counts | Tags |
|---|---|---|---|
| `nymbroker.retries` | counter | retry attempts of every [retry policy](resilience.md#how-nymbroker-uses-it) (transport reconnects, HTTP requests, broker-held deferred sends) | — |
| `nymbroker.circuit_breaker.state_changes` | counter | [circuit breaker](resilience.md#circuit-breaker) state changes | `circuit` (the consumer class, or `default`), `state` (`open`/`half_open`/`closed`) |
| `nymbroker.circuit_breaker.held` | counter | messages the broker held because their consumer's circuit was open | `circuit` |
| `nymbroker.circuit_breaker.rejected` | counter | calls a standalone `CircuitBreaker.ExecuteAsync` refused | `circuit` |
| `nymbroker.consumer.timeouts` | counter | consumers that passed their [timeout](resilience.md#timeout) | `consumer` |
| `nymbroker.throttle.waits` | counter | calls that waited for a consumer's or an endpoint's [rate or concurrency limit](resilience.md#rate-and-concurrency-limits) | `scope` (`consumer`/`endpoint`), `name`, `limit` (`rate`/`concurrency`) |
| `nymbroker.throttle.wait.duration` | histogram (ms) | how long those calls waited; calls that did not wait are not recorded | `scope`, `name` |

Tag values are endpoint, topic, type and consumer names, so the number of series stays small.

Useful views:

- **Throughput and errors per endpoint** — `received` and `failed` by `source`.
- **Where messages go** — `routed` by `destination`, `consumed` by `consumer`.
- **Poison messages** — `dead_lettered` by `reason`; alert on any increase.
- **Dead letters still waiting** — `queue.messages` with `state = failed`, and `queue.oldest_message.age` with `state = failed`, by `endpoint` ([queue monitoring](#queue-monitoring)). `dead_lettered` counts the events; these show what nobody has dealt with yet.
- **A failing downstream** — `circuit_breaker.state_changes` with `state = open`, and `consumer.timeouts`, by consumer.
- **A limit that is too tight** — `throttle.wait.duration` p95 by `name`; compare it with the transport's lease or lock time.
- **Latency** — `processing.duration` p95 by `source`; `result = retry` shows transient trouble.
- **Health** — alert on `health.checks` with `status = unhealthy` (or `degraded`) increasing, or on `health.status > 0`; `health.endpoint.failures` by `endpoint` and `reason` shows which endpoint fails and whether it is down (`unhealthy`), hanging (`timeout`) or broken (`error`: its check threw). The health metrics are only recorded when `CheckHealthAsync` runs — for example on each `/health` probe — so their resolution is the probe interval.
- **Backlog** — alert on `message.age` p95 or max by `source`: when consumers fall behind, messages wait longer and the age rises. It is measured against the sender's clock, so clock skew between hosts shifts it (negative values are recorded as 0). For depth:
  - SQL queue tables (SQLite, PostgreSQL, SQL Server, MySQL): `queue.messages` and `queue.oldest_message.age` with `state = pending`, with [queue monitoring](#queue-monitoring) on; or `GetCountsAsync` of the [queue inspector](#queue-inspection).
  - RabbitMQ: the queue's `messages_ready` (management API or the Prometheus plugin).
  - Azure Service Bus: `ActiveMessages` in Azure Monitor, or `ActiveMessageCount` from `ServiceBusAdministrationClient` (needs Manage rights).
  - Amazon SQS: CloudWatch `ApproximateNumberOfMessagesVisible` and `ApproximateAgeOfOldestMessage`.
  - Memory: not exposed.

## Traces

When a listener samples the `NymBroker` activity source, processing can create an activity `nymbroker.process` (kind *consumer*) with these tags:

| Tag | Value |
|---|---|
| `messaging.system` | `nymbroker` |
| `messaging.operation.type` | `process` |
| `messaging.destination.name`, `nymbroker.source` | endpoint the message was received on |
| `messaging.message.id` | envelope `id` |
| `messaging.conversation_id` | envelope `correlationId` |
| `messaging.message.type` | envelope `messageType` |
| `error.type` | exception type, when processing failed |

Activities started inside consumers (HTTP calls, database commands) become children of it, so a trace shows everything one message caused.

With a listener sampling the source, typed `PostAsync`, `PostDeferredAsync`, `PostBatchAsync` (one for the batch), each route or topic endpoint delivery, and the broker's dead-letter copy can create a `nymbroker.send` activity (kind *producer*). A batch of more than one message is tagged `messaging.batch.message_count`. Tags include `messaging.system`, `messaging.operation.type` = `send`, `messaging.destination.name`, and, where a single message context is available, `messaging.message.id`, `messaging.conversation_id`, `messaging.message.type`; `error.type` is set when the send failed. Raw posts, wire-tap copies and outbox writes do not start a send activity.

### Trace context across messages

The broker writes the W3C trace context of the sending activity into the envelope (`traceParent`, `traceState`), so a consumer in another process continues the producer's trace, on every transport:

```text
http-request (producer app)
└─ nymbroker.send  Orders
   └─ nymbroker.process  Orders (consumer app)
      ├─ SQL / HTTP calls of the consumer
      └─ nymbroker.send  Shipping (a route)
         └─ nymbroker.process  Shipping (next app)
```

- **Parent or link.** By default the context is the consumer activity's **parent**: one trace from the request to the last consumer. For long-running or fan-out flows, where one trace would grow without bound, make each processed message the root of a new trace with a **link** to the producer, as the OpenTelemetry messaging conventions recommend:

  ```csharp
  services.AddNymBroker()
      .ConfigureTracing(o => o.Continuation = TraceContinuation.Link)
  ```

- **Forwarding is hop by hop.** When send activities are created and propagation is enabled, a route, topic endpoint or dead-letter copy carries its own `nymbroker.send` context, a child of the message's `nymbroker.process`. The wire tap passes the original bytes to its destination, retaining the received context unless the destination has an output transformer.
- **Retries.** Every delivery of a message is its own `nymbroker.process` activity under the same producer activity, so redeliveries show as siblings.
- **Without a context** (for example an older envelope or a raw post without trace fields) or with one that is not a valid W3C `traceparent`, the process activity starts as before: a child of the current activity if there is one, else a new trace. The message itself is processed normally. A `traceState` longer than 512 characters is dropped. An [input transformer](pipeline-extensions.md#input-transformers) can set `RawMessageContext.TraceParent` / `TraceState` from an external message's own trace header.
- **When nobody traces.** If an app has no listener for the `NymBroker` source (no OpenTelemetry), it creates no activities; the envelopes it posts carry the caller's current activity if any, and the messages it forwards keep the context they arrived with, so a trace survives a hop that is not instrumented.
- **What it reveals.** `traceParent` holds random trace and span ids; `traceState` may hold vendor data. Turning propagation off removes trace context from envelopes the broker serializes, including route, topic and dead-letter forwards. Received context is still read. Raw posts and wire taps pass existing bytes through this setting, and an output transformer can produce its own content: sanitize these paths separately before crossing a trust boundary.

  ```csharp
  .ConfigureTracing(o => o.PropagateTraceContext = false)
  ```

`PostRawAsync` and `PostRawDeferredAsync` do not add trace context to the supplied bytes; optional splitting and destination output transformers can still change what is sent. With propagation enabled and a current W3C activity, a typed message posted through the [transactional outbox](transactional-outbox.md) carries the context of the code that wrote it (for example the web request), not of the dispatcher; a raw outbox post retains its supplied bytes.

## Logging

- While a message is processed, every log entry carries a scope with `MessageId`, `CorrelationId`, `MessageType` and `SourceEndpoint` — include scopes in your log output to correlate entries for one message.
- Failures are always logged: consumer failures and processing errors at `Error`, dead-lettering at `Warning` with its reason, listener loops that die at `Critical`. Nothing is swallowed silently.
- `AddMessageLoggingFilter()` logs every received message, including its payload, at `Debug`.
- Routing, topic delivery and splitting are logged per message at `Debug` ("Routed message …", "Topic '…' delivered …", "Splitting message …"); at `Information` the broker logs only startup, shutdown and other rare events. Use the `nymbroker.messages.routed` metric to watch routing in production.
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

- Each endpoint's `HealthCheckAsync()` (connectivity to its database or broker, whether its listener is running) runs **in parallel**, so the call takes about as long as the slowest endpoint. The database, SQS, Service Bus and HTTP endpoints probe their server asynchronously with a 5-second timeout, so a slow server holds no thread while the probe waits.
- An endpoint that has not answered within the overall **timeout** (default 10 s) is reported `Unhealthy` ("Health check timed out after …") and logged at `Warning`; its check finishes in the background.
- The call **never throws**: an endpoint whose health check throws is reported `Unhealthy` with the exception message and logged at `Warning`. Only an `OperationCanceledException` from your own token propagates.

| Situation | Aggregate `Status` |
|---|---|
| All endpoints healthy and the broker started | `Healthy` |
| Only endpoints marked non-critical are unhealthy | `Degraded` |
| An endpoint over its [dead-letter threshold](#dead-letter-thresholds) (and none unhealthy) | `Degraded`; that endpoint is `Degraded` with e.g. "3 dead letter(s) (threshold 0), oldest 2.5 h" |
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

## Queue inspection

`INymBroker.GetQueueInspector(endpointName)` counts and lists the messages in an endpoint's queue, for dashboards, support tools and replay. It returns an `IQueueInspector` (namespace `NymBroker.Core.Endpoint.Queue`), or `null` when the endpoint's transport doesn't offer one. The queue-table endpoints offer it: **SQLite, PostgreSQL, SQL Server and MySQL**. RabbitMQ, Azure Service Bus, Amazon SQS, Memory, File and HTTP return `null`: they cannot list messages and tell their states apart without receiving them, which would change what is being inspected. It throws `InvalidOperationException` for an unknown endpoint. It is read-only and safe to call while the broker runs. The [dashboard](dashboard.md) shows these counts and lists.

```csharp
var inspector = broker.GetQueueInspector("Orders") ?? throw new NotSupportedException();

QueueCounts counts = await inspector.GetCountsAsync(ct);   // Pending, InProgress, Deferred, Failed, Completed; counts.Active = the first three

string? cursor = null;
do
{
    QueuePage page = await inspector.GetMessagesAsync(QueueInspectionState.Failed, limit: 100, cursor,
        failedSince: DateTimeOffset.UtcNow.AddDays(-7), ct: ct);
    foreach (QueueMessageInfo m in page.Messages)
        Console.WriteLine($"{m.Id} attempts={m.Attempts} failed={m.FailedAt} error={m.LastError}");
    cursor = page.NextCursor;
} while (cursor is not null);
```

- `GetCountsAsync` reads all five counts as one snapshot.
- `GetMessagesAsync` lists `Pending`, `InProgress`, `Deferred` or `Failed` messages (`Completed` is only counted), at most `IQueueInspector.MaxPageSize` (1000) per page. Pass `NextCursor` back to continue; a cursor belongs to the state it came from. Pending and in-progress messages come in post order, deferred by delivery time (`DeliverAt`), failed by failure time (`FailedAt`; `failedSince` filters on it).
- `QueueMessageInfo.Id` is the message's own identifier: it stays the same when a deferred message becomes due, and does not depend on the transport's row numbers. `Payload` is the bytes as posted (the JSON envelope); `LastError` is the last failure text, `"{Reason}: {Description}"` for a dead-lettered message.
- Counts and pages are a snapshot at the time of the call; a message can change state right after.

## Queue monitoring

Queue counts are not collected unless something asks for them. `MonitorQueues` samples every endpoint that has a [queue inspector](#queue-inspection) (SQLite, PostgreSQL, SQL Server, MySQL) in the background, and publishes the results as the gauges `nymbroker.queue.messages` and `nymbroker.queue.oldest_message.age` ([Metrics](#metrics)):

```csharp
services.AddNymBroker()
    .AddSqlServerEndPoint("Orders", ordersSettings)
    .AddSqlServerEndPoint("DeadLetters", deadLetterSettings)
    .WithDeadLetterEndpoint("DeadLetters")
    .MonitorQueues(o =>
    {
        o.Interval = TimeSpan.FromSeconds(30);                                 // default 60 s, at least 1 s
        o.DeadLetterThreshold("Orders", maxCount: 0);                          // any failed message → Degraded
        o.DeadLetterThreshold("DeadLetters", maxCount: 100, maxAge: TimeSpan.FromHours(4));
    })
    .Build();
```

- The sampler runs as a hosted service. It samples once at start, then every `Interval`. Each sample is one count query per endpoint, plus one single-row query for each non-empty pending or failed state, to find its oldest message.
- The gauges and the health check read the **last sample**: a metrics scrape or a `/health` probe never queries the database. The ages are computed from the oldest message's time when they are read, so they keep growing between samples.
- An endpoint whose sample fails is logged at `Warning` and left out until a sample succeeds, so no stale numbers are reported.
- With several instances, each one samples the same tables and reports the same numbers. Aggregate with `max` by `endpoint`, not `sum`.
- RabbitMQ, Azure Service Bus, Amazon SQS, Memory and File have no inspector and are not sampled; use the transport's own metrics (see **Backlog** under [Metrics](#metrics)).

### Dead-letter thresholds

`DeadLetterThreshold(endpoint, maxCount, maxAge)` reports the endpoint **Degraded** in the [health check](#health-checks) while it holds more than `maxCount` dead letters, or its oldest dead letter is older than `maxAge`. Give at least one of them.

- What counts as a dead letter depends on the endpoint. On a queue table it is a failed message, whether the table's native dead-lettering or `MaxRetryCount` put it there. On the broker's dead-letter endpoint (`WithDeadLetterEndpoint`) it is every message waiting there.
- Degraded, not Unhealthy: ASP.NET Core answers `/health` with `200` for Degraded by default, so an orchestrator doesn't restart the app over a backlog that a restart can't fix, while monitoring and health UIs see it.
- An endpoint that is unreachable stays `Unhealthy`. Until the first sample there is nothing to compare, so the endpoint keeps its own status.
- `Build()` throws for a threshold on an endpoint that isn't registered. A threshold on an endpoint without an inspector is logged at `Warning` when the sampler starts, and is never checked.

### Alert examples

With the OpenTelemetry Prometheus exporter, the gauges are named `nymbroker_queue_messages` and `nymbroker_queue_oldest_message_age_seconds`:

```promql
# Any dead letter waiting on Orders for 5 minutes
max by (endpoint) (nymbroker_queue_messages{endpoint="Orders", state="failed"}) > 0

# A dead letter older than a day, anywhere
max by (endpoint) (nymbroker_queue_oldest_message_age_seconds{state="failed"}) > 86400

# A stuck queue: the oldest pending message waits more than 10 minutes
max by (endpoint) (nymbroker_queue_oldest_message_age_seconds{state="pending"}) > 600

# A growing backlog: 1000 more pending messages than an hour ago
max by (endpoint) (nymbroker_queue_messages{state="pending"})
  - max by (endpoint) (nymbroker_queue_messages{state="pending"} offset 1h) > 1000

# The broker reports Degraded (a threshold, or a non-critical endpoint down)
nymbroker_health_status == 1
```

Use `for: 5m` on the rule so that one sample doesn't page anybody. Azure Monitor and other OTLP back ends get the same instruments under their dotted names.
