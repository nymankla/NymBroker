# Messages and consumers

[← User guide](user-guide.md)

## Messages

A message is any reference type — a class or a record — that `System.Text.Json` can serialize. Properties are written in camelCase and read case-insensitively.

```csharp
[MessageName("order.created")]
public sealed record OrderCreated(string OrderId, string Customer, decimal Amount, string Priority = "normal");
```

### Message type names

Every envelope carries the message's **type name**, which the receiving side uses to find the CLR type and its consumer:

1. the `[MessageName("…")]` value, if the class has one, otherwise
2. the class's full name, e.g. `MyApp.Orders.OrderCreated`.

Use `[MessageName]` for anything that crosses process boundaries. It decouples the wire name from your namespace, so producer and consumer can live in different assemblies and you can rename or move the class without breaking messages already in a queue.

Types are registered automatically when you register a consumer, subscriber, route or topic for them. A message whose type name is unknown on the receiving side is not dispatched (a warning is logged), unless a type-independent route picks it up.

## The envelope

On the wire every message is a JSON envelope:

```json
{
  "id": "6f1c…",
  "correlationId": "b2a9…",
  "address": { "to": "Orders", "from": "WebApi" },
  "messageType": "order.created",
  "created": "2026-10-06T08:15:00Z",
  "message": { "orderId": "ORD-1", "customer": "Alice", "amount": 49.9, "priority": "high" }
}
```

| Field | Meaning |
|---|---|
| `id` | Unique per message. Used by the idempotent receiver to drop duplicates. |
| `correlationId` | Groups related messages. New per message by default; `PostBatchAsync` can set one for a whole batch. |
| `address.to` / `address.from` | The endpoint it was posted to / the endpoint it was received from (set by the broker on arrival). |
| `messageType` | The type name described above. |
| `created` | UTC creation time; used for message expiry (TTL) and age-based routes. |
| `message` | Your message. |
| `deadLetter` | Only on dead-lettered messages: why it failed (see [Reliability](reliability.md#why-a-message-was-dead-lettered)). |

The broker writes and reads the envelope for you; you only see it if you inspect a queue or table directly, or post raw bytes yourself.

## Consumers

A consumer handles messages of one or more types. Implement `IConsume<T>` and register the class with `AddConsumer<T>()`:

```csharp
public sealed class OrderConsumer(IOrderRepository orders, ILogger<OrderConsumer> logger)
    : IConsume<OrderCreated>, IConsume<OrderCancelled>
{
    public async Task ConsumeAsync(OrderCreated message, IMessageContext context, CancellationToken ct = default)
    {
        await orders.AddAsync(message, ct);
        logger.LogInformation("Order {Id} stored (message {MessageId} from {Source})",
            message.OrderId, context.Id, context.Address?.From);
    }

    public Task ConsumeAsync(OrderCancelled message, IMessageContext context, CancellationToken ct = default)
        => orders.CancelAsync(message.OrderId, ct);
}

services.AddNymBroker()
    .AddSqliteEndPoint("Orders", settings)
    .AddConsumer<OrderConsumer>()   // registers both IConsume<> interfaces
    .Build();
```

Rules to know:

- **One consumer per message type.** Registering a second, different consumer for a type that already has one throws `InvalidOperationException` at `AddConsumer<T>()`, naming both consumers. Registering the same consumer twice is harmless. Two different consumer classes with the same class name (in different namespaces) also throw, because consumers are registered by class name. To have several independent handlers for one message, use a topic with `ISubscribe<T>` subscribers ([Routing and publish/subscribe](routing-and-pubsub.md#topics-and-subscribers)).
- **A fresh DI scope per message.** Each dispatch creates its own `IServiceScope`, so consumers can depend on scoped services such as an EF Core `DbContext`. Consumers are registered as transient.
- **Routes and topics come first.** A message that matches a route or a topic is forwarded there and is **not** given to the consumer. See [what decides where a message goes](routing-and-pubsub.md#what-decides-where-a-message-goes).
- **Throwing means "failed".** If `ConsumeAsync` throws, the message is retried by its transport and eventually dead-lettered (or sent to the broker's dead-letter endpoint). Return normally only when the message is fully handled. See [Reliability](reliability.md).
- **Honour the cancellation token.** It is cancelled when the broker stops.

## IMessageContext

Every consumer and subscriber call receives the envelope's metadata:

| Property | Type | Notes |
|---|---|---|
| `Id` | `Guid` | Message id |
| `CorrelationId` | `Guid` | Correlation id |
| `Address` | `EndpointAddress?` | `To` — where it was posted; `From` — the endpoint it was received on |
| `MessageType` | `string?` | Wire type name |
| `Created` | `DateTime` | UTC creation time |
| `DeadLetter` | `DeadLetterInfo?` | Set only when the message was dead-lettered — useful when consuming from a dead-letter endpoint |

`context.Address.From` is how one consumer can behave differently depending on where a message came from — for example a consumer that also reads from a dead-letter endpoint.

## Sending from a consumer

Inject `INymBroker` to send follow-up messages:

```csharp
public sealed class PaymentConsumer(INymBroker broker) : IConsume<OrderCreated>
{
    public async Task ConsumeAsync(OrderCreated message, IMessageContext context, CancellationToken ct = default)
    {
        await broker.PostAsync("Payments", new ChargeCard(message.OrderId, message.Amount), ct);
    }
}
```

Be careful not to post a message back to the endpoint it came from with the same type unless that is intended — it will be processed again.
