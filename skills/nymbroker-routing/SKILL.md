---
name: nymbroker-routing
description: Add NymBroker routes (forward messages to another endpoint by type, source or content) and topics (fan out to several endpoints or ISubscribe<T> subscribers), with guards against routing loops. Use when the user wants to route, forward, copy, fan out, broadcast, publish/subscribe, split traffic by priority/content, or send a message to several places with NymBroker, or reports messages being processed over and over.
---

# NymBroker routing and publish/subscribe

When a message arrives on an endpoint, the broker decides where it goes:

1. **All matching routes** forward a copy to their destination endpoint.
2. **All matching topics** deliver to their subscriber endpoints and `ISubscribe<T>` subscribers.
3. Only if **nothing** matched does the **consumer** (`IConsume<T>`) for the type get it. A routed or topic-matched message is **not** given to the consumer.

Docs: https://github.com/nymankla/NymBroker/blob/master/docs/routing-and-pubsub.md

## 1. Clarify the goal

Settle (ask only what is unclear):

- What should happen: **forward** to another queue/system (route), **several independent handlers** in this app (topic + subscribers), a **copy for audit** of every raw message (wire tap — `AddWireTap`, see nymbroker-setup), or **one handler** (just a consumer — no routing needed).
- Which message types, which source endpoints, which content condition.
- Whether the message should *also* still reach its consumer. If yes, use a topic with an `ISubscribe<T>` subscriber instead of a route, or route the copy to an endpoint that has the consumer.
- For every destination endpoint: **does this broker also listen on it?** (registered without `EndpointMode.WriteOnly`). If yes, a loop guard is required (section 4).

## 2. Routes

Routes are added on `INymBroker` (namespace `NymBroker.Core.Impl`) after the service provider is built — in ASP.NET Core right after `var app = builder.Build();`, in a worker after `host = builder.Build()`. Add them before `app.Run()` / `host.RunAsync()` so they exist when messages start flowing.

```csharp
var broker = app.Services.GetRequiredService<INymBroker>();

// Every OrderCreated received on "WebOrders" also goes to "Archive" (WriteOnly).
broker.Route<OrderCreated>().To("Archive").WhenFrom("WebOrders").Build();

// High priority or large orders go to "Priority", the rest to "Standard".
broker.Route<OrderCreated>()
    .To("Priority")
    .WhenFrom("Inbox")
    .Or(new JsonRouteCondition(m => m.GetProperty("priority").GetString() == "high"),
        new JsonRouteCondition(m => m.GetProperty("amount").GetDecimal() > 10_000m))
    .Build();

// Any message type older than 5 minutes → "Stale".
broker.Route().To("Stale").WhenMessageIsOlderThan(TimeSpan.FromMinutes(5)).Build();
```

| Condition | Matches when |
|---|---|
| `.WhenFrom("X")` / `.WhenNotFrom("X")` | received on / not on endpoint X (one source filter per route; a second call replaces it) |
| `.When(json => …)` | predicate on the **message payload** `JsonElement` (camelCase names, not the envelope) |
| `.WhenMessageIsOlderThan(age)` | envelope `created` older than `age` |
| `.And(a, b)` / `.Or(a, b)` | condition objects from `NymBroker.Core.Route`: `JsonRouteCondition`, `FromRouteCondition`, `NotFromRouteCondition`, `MessageAgeRouteCondition`, `AndRouteCondition`, `OrRouteCondition` |

- All conditions in one chain are **AND-ed**. Use `.Or(...)` for alternatives.
- In `When` predicates use `TryGetProperty` when a property may be missing — `GetProperty` throws, which fails the message.
- `.Build()` is required; it throws if `.To(...)` is missing. `broker.Route()` (no type) matches every message type.
- Custom logic: implement `IRouteCondition`, or subclass `RouteContext` and override `Evaluate`, then `broker.Route(() => new MyRouteContext()).To("X").Build()`.
- Don't use `.Transform(...)` — it is obsolete and does nothing.

## 3. Topics

Topics are configured on the builder, inside the `AddNymBroker()` chain:

```csharp
builder.Services.AddNymBroker()
    .AddSqlServerEndPoint("Orders", ordersSettings)
    .AddFileEndPoint("Audit", new FileSettings { PostPath = "audit" }, EndpointMode.WriteOnly)
    .AddTopic<OrderCreated>("orders.created")
        .SubscribeWith<SendConfirmationEmail>()     // ISubscribe<OrderCreated>, in-process
        .SubscribeWith<UpdateSalesStatistics>()
        .SubscribeTo("Audit")                       // a copy to an endpoint
        .When(m => m.GetProperty("amount").GetDecimal() > 0)   // optional, AND-ed
        .Build()                                    // back to the broker builder
    .Build();
```

- Triggered implicitly by every matching message received on any endpoint or `PublishAsync(message)`; explicitly by `PublishAsync("orders.created", message)` (skips routes and consumers).
- Subscribers implement `ISubscribe<T>.ReceiveAsync(message, context, ct)` (namespace `NymBroker.Core.Consume`); see the nymbroker-consumer skill. They run in sequence in one DI scope; if one throws, the delivery is retried, so all must be idempotent.
- Topics with endpoint subscribers can also come from config (`"Topics": [{ "TopicName", "MessageType", "SubscriberEndpoints": [...] }]`); subscribers and conditions stay in code.

## 4. Loop guard (required check)

Any destination this broker listens on processes the copy again — and the copy matches the same route or topic again, forever. For **each** route and topic destination:

- Destination only written to → register it with `EndpointMode.WriteOnly`. Preferred.
- Destination also consumed by this app → add `.WhenFrom("<source>")` (route only fires for messages from the original source) or `.WhenNotFrom("<destination>")`. For a topic: `.When(new NotFromRouteCondition("<destination>"))`.
- `StartAsync` throws if a route, topic, dead-letter or wire-tap destination is `ReadOnly`.

## 5. Finish

1. `dotnet build`.
2. Walk the user through where each message type now goes, per source endpoint, and confirm which ones no longer reach their consumer.
3. Suggest a test with Memory endpoints (nymbroker-testing skill) that posts one message per branch.
