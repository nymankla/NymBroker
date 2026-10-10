# MediatR patterns with NymBroker

[← User guide](user-guide.md)

Commands and notifications map directly onto NymBroker; queries do not, and should not. This page shows each MediatR pattern implemented with NymBroker, why it is done that way, and when MediatR or a plain method call is the better tool. The runnable code is [`samples/NymBroker.MediatRSample`](../samples/NymBroker.MediatRSample) — an order API that also demonstrates CQRS.

## The mapping

| MediatR | NymBroker | Fit |
|---|---|---|
| `IRequest` + `IRequestHandler<T>` (command, no response), `mediator.Send(command)` | message + `IConsume<T>`, `broker.PostAsync("Commands", command)` | Direct. Both allow exactly one handler per type. |
| `INotification` + `INotificationHandler<T>`, `mediator.Publish(notification)` | message + topic with `ISubscribe<T>` subscribers, `broker.PublishAsync(notification)` | Direct. |
| `IRequest<TResponse>` (query), `var result = await mediator.Send(query)` | **No counterpart** — NymBroker has no request/reply | Call a query service directly (see [Queries](#queries)). |
| `IPipelineBehavior<TRequest, TResponse>` | `IMessageFilter`, idempotent receiver, wire tap, built-in metrics and tracing | Partial: steps run *before* dispatch; nothing wraps the handler. |
| Handler exception reaches the caller of `Send` | Handler exception → retry by the transport → dead letter | Different by design. |

## Commands

```csharp
// API: validate, enqueue, answer 202 with where to look
await broker.PostAsync("Commands", new CreateOrder(orderId, request.Customer, request.Amount), ct);
return Results.Accepted($"/orders/{orderId}", new { orderId });

// Handler: one per command type, a fresh DI scope per message
public sealed class CreateOrderHandler(OrderStore store, INymBroker broker) : IConsume<CreateOrder> { … }
```

**Why NymBroker:** a command is work to be done, and with NymBroker that work goes through a queue. Register the `Commands` endpoint as SQLite instead of Memory — one line in the sample — and a command survives a crash, is retried when the handler throws, ends up as a dead letter when it keeps failing, and can be handled by a separate worker process. MediatR calls the handler in the caller's thread; if the process dies, the command is gone.

**How it differs:** `PostAsync` returns once the command is queued, not when it is handled. The caller gets no return value and never sees the handler's exception. So the API chooses the order id itself, answers `202 Accepted` with the URL to query, and validates input *before* posting — a handler that rejects input can only fail the message, not the HTTP request.

## Notifications

```csharp
.AddTopic<OrderCreated>("orders.events")
    .SubscribeWith<OrderSummaryProjection>()   // updates the read model
    .SubscribeWith<SendConfirmationEmail>()    // independent side effect
    .Build()

await broker.PublishAsync(new OrderCreated(…), ct);   // from the command handler
```

**Why NymBroker:** same shape as MediatR's `Publish`, plus a topic can also forward a copy to an endpoint (`SubscribeTo("Audit")`) — another queue, another process — without changing the publisher.

**How it differs:** subscribers of one delivery run in sequence in a shared DI scope. If one throws, the others still run, and the delivery goes to the dead-letter endpoint (or is retried when it arrived over a durable endpoint) — `PublishAsync` itself does not throw for a subscriber failure. Subscribers must therefore be idempotent.

## Queries

```csharp
app.MapGet("/orders/{id:guid}", (Guid id, OrderQueries queries) =>
    queries.Get(id) is { } summary ? Results.Ok(summary) : Results.NotFound());
```

Queries go straight to a query service; the broker is not involved. That is deliberate:

- A query needs its answer now. Putting it on a queue adds a hop, a serialization round trip and a place to fail, and gains nothing: a read has no side effect to retry or dead-letter.
- Request/reply over a queue needs a reply channel, correlation ids and timeouts — machinery a method call does not need.
- In CQRS the read side is meant to be simple and fast. An injected `OrderQueries` (or a `DbContext` with no tracking) is easy to test and to cache.

## Pipeline behaviors

| Cross-cutting need | MediatR | NymBroker |
|---|---|---|
| Logging / auditing | `LoggingBehavior` | An `IMessageFilter` (`AuditFilter` in the sample) or `AddMessageLoggingFilter()`; a log scope with message id and type is always set |
| Validation | `ValidationBehavior` throws to the caller | Validate before `PostAsync` (the sample's API returns 400). A filter that returns `null` drops the message silently — fine for unwanted traffic, wrong for user errors |
| Retry on transient failure | A Polly behavior | The transport retries (`MaxRetryCount`, `MaxDeliveryCount`), then dead-letters |
| Duplicate suppression | Hand-written | `AddIdempotentReceiver()` or a durable store |
| Metrics and tracing | Hand-written | Built in: meter `NymBroker`, activity source `NymBroker` ([Observability](observability.md)) |
| Transactions / unit of work around the handler | `TransactionBehavior` | Inside the handler (each message has its own DI scope), or a decorator registered in DI |

Filters run for every received message before routing and dispatch; they cannot run code after the handler. Wrap the handler class with a decorator when you need before-and-after logic.

## CQRS with NymBroker

The sample implements the full loop:

1. `POST /orders` validates and posts `CreateOrder` → `202 Accepted`.
2. `CreateOrderHandler` writes the write model and publishes `OrderCreated`.
3. `OrderSummaryProjection` updates the read model; `SendConfirmationEmail` runs alongside.
4. `GET /orders/{id}` reads the read model — `404` until the projection has run (eventual consistency; the demo measures the delay, typically tens of milliseconds in memory).

| | Pros | Cons |
|---|---|---|
| **Scaling** | Write and read sides scale and are optimized separately; commands can be handled by any number of workers sharing a durable queue | More moving parts to deploy and monitor |
| **Reliability** | Commands survive restarts, are retried, and are kept as dead letters when they fail | Handlers must be idempotent: at-least-once delivery means a message can arrive twice |
| **Consistency** | Read models are shaped for their queries and never block writes | The read model lags behind the write model; the UI must cope ("processing…", polling, or returning the id) |
| **Feedback** | The API answers fast (202) under load spikes — the queue absorbs them | No return value or exception for the caller; failures show up in dead letters, logs and metrics, not in the HTTP response |
| **Evolution** | New subscribers (search index, analytics, integrations) attach to existing events without touching the publisher | Events become a contract; changing them needs versioning (new `[MessageName]`) |
| **Operations** | Built-in metrics, traces and health checks per endpoint | Someone has to watch the dead letters and the backlog |

**When CQRS does not pay off:** plain CRUD with one model, no load asymmetry and users who expect to see their change immediately. Use a service class and a database.

## Choosing between them

| Use NymBroker when | Use MediatR or a plain call when |
|---|---|
| The work can happen after the request returns | The caller needs the result or the exception now |
| It must survive a crash, be retried, or be kept when it fails | Everything is synchronous, in one process, and a failure should simply fail the request |
| Work should spread over several processes or machines | You want behaviors that wrap the handler (before and after) |
| You may move to RabbitMQ or Azure Service Bus later without touching handlers | The indirection is only for organizing code |

**Licensing:** MediatR 13 and later (July 2025) are dual-licensed under the Reciprocal Public License 1.5 and a commercial license; the free Community edition covers organizations under USD 5 million in gross annual revenue, and 12.5 was the last Apache 2.0 release ([announcement](https://www.jimmybogard.com/automapper-and-mediatr-commercial-editions-launch-today/)). NymBroker is MIT. Check the current terms for your situation; licensing should not be the only reason to switch.

## Run the sample

```bash
dotnet run --project samples/NymBroker.MediatRSample -- --demo            # CQRS round trip with checks, then exits
dotnet run --project samples/NymBroker.MediatRSample -- --demo --sqlite   # same, commands in a SQLite queue
dotnet run --project samples/NymBroker.MediatRSample                      # API on http://localhost:5000
```

See the [sample README](../samples/NymBroker.MediatRSample/README.md) for the HTTP calls.
