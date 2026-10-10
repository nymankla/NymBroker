# NymBroker.MediatRSample

MediatR patterns — command, notification, query, pipeline behavior — implemented with NymBroker, wired into a small CQRS order API. The reasoning behind each choice, and the pros and cons, are in [docs/mediatr-comparison.md](../../docs/mediatr-comparison.md).

| MediatR | Here |
|---|---|
| `IRequest` / `IRequestHandler` | `CreateOrder` → `CreateOrderHandler : IConsume<CreateOrder>`, posted to the `Commands` endpoint |
| `INotification` / `INotificationHandler` | `OrderCreated` → topic `orders.events` → `OrderSummaryProjection`, `SendConfirmationEmail` |
| `IRequest<TResponse>` (query) | `OrderQueries`, called directly by `GET /orders/{id}` — no broker |
| `IPipelineBehavior` | `AuditFilter : IMessageFilter`, `AddIdempotentReceiver()`, the dead-letter endpoint |

## Run

```bash
# Self-checking demo: posts an invalid and a valid order over HTTP, waits for the read model, exits 0 or 1
dotnet run --project samples/NymBroker.MediatRSample -- --demo
dotnet run --project samples/NymBroker.MediatRSample -- --demo --sqlite

# The API (commands in memory, or add --sqlite for a durable queue in mediatr-sample.db)
dotnet run --project samples/NymBroker.MediatRSample
```

With the API running:

```bash
curl -i -X POST http://localhost:5000/orders -H "Content-Type: application/json" -d '{"customer":"Alice","amount":499.90}'
# 202 Accepted, Location: /orders/<id>

curl http://localhost:5000/orders/<id>    # 404 until the projection has run, then the summary
curl http://localhost:5000/orders         # all summaries, newest first
```

## What to look at

- [`Program.cs`](Program.cs) — the broker registration (`--sqlite` swaps one line), the command endpoint with validation, the query endpoints, and the demo.
- [`Orders/Handlers.cs`](Orders/Handlers.cs) — the command handler, the two notification subscribers and the filter, each with the MediatR interface it replaces.
- [`Orders/Models.cs`](Orders/Models.cs) — write model and read model (in memory to keep the sample self-contained; the queue is what `--sqlite` makes durable).

Failed messages are written as JSON to `dead-letters/`.
