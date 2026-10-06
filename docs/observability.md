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

Every endpoint implements `HealthCheck()` (connectivity to its database or broker, whether its listener is running). It never throws. Endpoints are registered as keyed `IEndPoint` services, so you can expose them through ASP.NET Core health checks:

```csharp
using Microsoft.Extensions.Diagnostics.HealthChecks;
using NymBroker.Core.Endpoint;

public sealed class EndpointHealthCheck(IServiceProvider services, string endpointName) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct = default)
    {
        var result = services.GetRequiredKeyedService<IEndPoint>(endpointName).HealthCheck();
        return Task.FromResult(result.IsHealthy ? HealthCheckResult.Healthy() : HealthCheckResult.Unhealthy(result.Message));
    }
}

builder.Services.AddHealthChecks()
    .AddTypeActivatedCheck<EndpointHealthCheck>("orders-endpoint", "Orders");
```

`HealthCheck()` is synchronous and the database and Service Bus endpoints contact their server (with a 5-second timeout), so don't call it on a hot path.
