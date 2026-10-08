---
name: nymbroker-consumer
description: Create a NymBroker consumer (IConsume<T>) or topic subscriber (ISubscribe<T>) that handles a message type, register it with the broker, and make it safe under retries and at-least-once delivery. Use when the user wants to handle, consume, process, subscribe to or react to a NymBroker message, add a handler or worker for a message, or asks why a message is not being handled.
---

# Create a NymBroker consumer

A consumer is a class implementing `IConsume<T>` for one or more message types. The broker receives the message on an endpoint, creates a fresh DI scope and calls `ConsumeAsync`. Returning normally means **done**; throwing means **failed** — the transport retries and eventually dead-letters it.

Docs: https://github.com/nymankla/NymBroker/blob/master/docs/messages-and-consumers.md · https://github.com/nymankla/NymBroker/blob/master/docs/reliability.md

## 1. Find out what is needed

Settle from the request and the code (ask only what is unclear):

- **Which message type(s).** It must exist; if not, create it with the **nymbroker-message** skill.
- **One handler or several?** NymBroker allows **one consumer per message type** — a second consumer for the same type throws at startup. If the message already has a consumer (search for `IConsume<TheMessage>`), or several independent reactions are wanted (send email *and* update stats), use **topic subscribers** (section 4) or extend the existing consumer.
- **Which endpoint it arrives on** — the broker listens on every endpoint that is not `WriteOnly`. Make sure the app registers that endpoint in `ReadWrite` (default) or `ReadOnly` mode.
- NymBroker must already be set up (`AddNymBroker()` present). If not, use **nymbroker-setup** first.

## 2. Write the consumer

```csharp
using Microsoft.Extensions.Logging;
using NymBroker.Core.Consume;
using NymBroker.Core.Message;

namespace MyApp.Orders;

public sealed class OrderCreatedConsumer(IOrderRepository orders, ILogger<OrderCreatedConsumer> logger)
    : IConsume<OrderCreated>
{
    public async Task ConsumeAsync(OrderCreated message, IMessageContext context, CancellationToken ct = default)
    {
        if (await orders.ExistsAsync(message.OrderId, ct))
        {
            logger.LogInformation("Order {OrderId} already stored, skipping (message {MessageId})", message.OrderId, context.Id);
            return;                                    // idempotent: a redelivery is harmless
        }

        await orders.AddAsync(message, ct);
        logger.LogInformation("Order {OrderId} stored (message {MessageId} from {Source})",
            message.OrderId, context.Id, context.Address?.From);
    }
}
```

Register it on the broker builder (next to the existing `AddNymBroker()` chain, before `.Build()`):

```csharp
builder.Services.AddNymBroker()
    // … endpoints …
    .AddConsumer<OrderCreatedConsumer>()   // registers every IConsume<T> the class implements
    .Build();
```

Rules:

- **Dependencies by constructor injection.** Each message gets its own DI scope, so scoped services (EF Core `DbContext`, unit of work) are fine. Consumers are transient — keep no state in fields between messages.
- **Throw on failure, return on success.** Don't catch-and-log exceptions you can't handle: swallowing them completes the message and it is lost. Let them propagate so the transport retries (SQL tables: `MaxRetryCount`, Service Bus: `MaxDeliveryCount`, RabbitMQ: one redelivery) and then dead-letters. For a message that can *never* succeed (invalid data), log and return, or post it to an error endpoint yourself — retrying won't help.
- **Be idempotent.** Delivery is at-least-once: after a crash, lost lock or retry the same message can arrive twice. Check whether the work was already done (by business key or `context.Id`), use upserts, or enable the broker's duplicate detection (`AddIdempotentReceiver(...)` or a durable store — see nymbroker-setup).
- **Pass the `CancellationToken`** to every async call; it is cancelled when the broker stops.
- **Class names must be unique** across consumers (they are registered by simple class name), even in different namespaces.
- One class may implement several `IConsume<T>` for related messages (`IConsume<OrderCreated>, IConsume<OrderCancelled>`).

## 3. Useful context

`IMessageContext`: `Id` (message id), `CorrelationId`, `Address.From` (endpoint it was received on) / `Address.To`, `MessageType`, `Created` (UTC), `DeadLetter` (why it was dead-lettered, when consuming a dead-letter endpoint).

Send follow-up messages by injecting `INymBroker` (namespace `NymBroker.Core.Impl`):

```csharp
await broker.PostAsync("Payments", new ChargeCard(message.OrderId, message.Amount), ct);
```

Never post the same message type back to the endpoint it came from unless intended — it will be processed again (routing loop).

## 4. Several handlers: topic subscribers

```csharp
using NymBroker.Core.Consume;

public sealed class SendConfirmationEmail(IEmailSender email) : ISubscribe<OrderCreated>
{
    public Task ReceiveAsync(OrderCreated message, IMessageContext context, CancellationToken ct = default)
        => email.SendOrderConfirmationAsync(message.OrderId, ct);
}

builder.Services.AddNymBroker()
    // … endpoints …
    .AddTopic<OrderCreated>("orders.created")
        .SubscribeWith<SendConfirmationEmail>()
        .SubscribeWith<UpdateSalesStatistics>()
        .Build()                                // back to the broker builder
    .Build();
```

- Every `OrderCreated` received on any endpoint (or `PublishAsync(message)`) goes to the topic's subscribers; `PublishAsync("orders.created", message)` targets the topic directly.
- A message that matches a topic or route is **not** also given to a consumer — pick one model per message type.
- Subscribers of one delivery run in sequence in a shared scope; if any throws, the whole delivery is retried, so each subscriber must be idempotent.

## 5. Test it

Use the in-memory endpoint and post a message:

```csharp
var services = new ServiceCollection().AddLogging();
services.AddSingleton<IOrderRepository, FakeOrderRepository>();
services.AddNymBroker()
    .AddMemoryEndPoint("Orders")
    .AddConsumer<OrderCreatedConsumer>()
    .Build();

await using var provider = services.BuildServiceProvider();
var broker = provider.GetRequiredService<INymBroker>();
await broker.StartAsync();
await broker.PostAsync("Orders", new OrderCreated("ORD-1", "C-1", 10m, DateTimeOffset.UtcNow));
// wait for the fake repository to record the order (e.g. a TaskCompletionSource with a timeout), then assert
await broker.StopAsync();
```

Or unit-test `ConsumeAsync` directly with a fake `IMessageContext`.

## 6. Finish

`dotnet build` (and tests). Tell the user which endpoint the consumer listens on and how failures are retried / dead-lettered for that transport.

## Not being called? Check

- The consumer is registered with `AddConsumer<T>()` in the same `AddNymBroker()` chain, and `.Build()` is called.
- The message arrives on an endpoint that isn't `WriteOnly`, and the host is started.
- The sender's `[MessageName]` (or full type name) matches the consumer's message type — otherwise a "no type/consumer" warning is logged.
- No route or topic is taking the message first.
