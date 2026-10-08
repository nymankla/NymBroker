---
name: nymbroker-message
description: Create a NymBroker message type — a C# record with a stable [MessageName] wire name, JSON-friendly properties and a place in a shared contracts project — and show how to send it with INymBroker. Use when the user wants to add, define, create or design a message, event, command or contract for NymBroker, or asks how to send/post/publish one.
---

# Create a NymBroker message

A NymBroker message is any class or record that `System.Text.Json` can serialize. The broker wraps it in a JSON envelope (id, correlation id, type name, created time) — you never write the envelope yourself.

Docs: https://github.com/nymankla/NymBroker/blob/master/docs/messages-and-consumers.md

## 1. Find out what is needed

From the request and the code, settle (ask only what is unclear):

- **Name and kind** — an event (something happened: `OrderCreated`, past tense) or a command (do something: `ChargeCard`, imperative).
- **Fields** and their types.
- **Where it goes** — if producer and consumer are separate projects or services, the message belongs in a shared contracts project/package both reference. Look for an existing one (`*.Contracts`, `*.Messages`) and follow its folder and namespace conventions.
- NymBroker must already be set up (`AddNymBroker()` present, package `NymBroker` referenced). If not, use the **nymbroker-setup** skill first.

## 2. Write the message

```csharp
using NymBroker.Core.Message;

namespace MyApp.Contracts.Orders;

[MessageName("orders.order-created")]
public sealed record OrderCreated(
    string OrderId,
    string CustomerId,
    decimal Amount,
    DateTimeOffset PlacedAt,
    string Priority = "normal");
```

Rules:

- **Always add `[MessageName("…")]`.** The envelope's `messageType` is this name, or else the CLR full name (namespace + class). With a stable name you can move or rename the class without breaking messages already sitting in a queue, and producer and consumer can use different assemblies. Use lowercase, dot-separated `<area>.<message>` names, unique across the system. Never change a name once messages with it may exist.
- **Reference type only** — `sealed record` (preferred) or class. Structs are not allowed (`where T : class`).
- **Serializable by System.Text.Json**: public properties or a primary constructor; no `Newtonsoft` attributes; avoid polymorphic/abstract properties, delegates, streams, `DbContext` entities with navigation loops. Properties are written camelCase and read case-insensitively.
- **Data, not behaviour** — no services or methods with side effects. Prefer `string` ids, `decimal` for money, `DateTimeOffset` (or UTC `DateTime`) for times, enums only if both sides share the type.
- **Keep it small.** Send ids and the facts the consumer needs, not whole object graphs. For payloads that may exceed the transport's size limit, post with `splitThresholdBytes` (see below).
- **Evolve compatibly**: add new properties as optional (default value) so old messages still deserialize; don't rename or retype existing ones. For a breaking change create a new message with a new name (`orders.order-created.v2`).

## 3. Send it

Inject `INymBroker` (namespace `NymBroker.Core.Impl`):

```csharp
public sealed class CheckoutService(INymBroker broker)
{
    public Task PlaceAsync(Order order, CancellationToken ct) =>
        broker.PostAsync("Orders", new OrderCreated(order.Id, order.CustomerId, order.Total, DateTimeOffset.UtcNow), ct);
}
```

| Need | Call |
|---|---|
| send to one endpoint | `PostAsync("Orders", message, ct)` |
| many messages, one round trip (SQL endpoints, Service Bus) | `PostBatchAsync("Orders", messages, ct)` |
| large message, split and compressed transparently | `PostAsync("Orders", message, ct, splitThresholdBytes: 200_000)` |
| deliver to topic subscribers in this process | `PublishAsync(message, ct)` or `PublishAsync("orders.events", message, ct)` |

The endpoint name must be one registered with `Add…EndPoint` (or in config). Posting to an unknown name throws "No endpoint registered".

## 4. Handle it

A message type is registered automatically when a consumer, subscriber, route or topic uses it. On the receiving side a message whose type has no consumer is logged as a warning and completed (dropped). Create the handler with the **nymbroker-consumer** skill.

## 5. Finish

`dotnet build`, then tell the user the message name you chose and which endpoint it is posted to.
