# MessageBroker

A .NET 10 enterprise message processing framework following [Enterprise Integration Patterns](https://www.enterpriseintegrationpatterns.com/). Messages flow as JSON envelopes through a configurable pipeline of endpoints, filters, routers, and consumers.

```
Source Endpoint → Deserialize → Filter → Router → Consumer / Destination Endpoint
                                              ↕
                                    Aggregator / Splitter
```

## Features

- **Multiple transports** — RabbitMQ, File system, and in-process Memory endpoint
- **Fluent routing API** — type-safe, composable route conditions
- **Typed consumers** — implement `IConsume<T>`, optionally handle multiple message types in one class
- **Scheduled actions** — interval-based or Cron expression (via [Cronos](https://github.com/HangfireIO/Cronos))
- **JSON config file** — declare endpoint topology in `queuesettings.json`; consumers and routes stay in code
- **High performance** — compiled Expression dispatch, RecyclableMemoryStream, lock-free ImmutableCollections
- **Scoped consumers** — each message dispatch gets its own DI scope

## Solution layout

| Project | Purpose |
|---|---|
| `MessageBroker.Core` | Framework core — no RabbitMQ dependency |
| `MessageBroker.RabbitMq` | Optional RabbitMQ transport (add when needed) |
| `MessageBroker.Tests` | xUnit tests |
| `samples/MessageBroker.Sample` | Fluent API demo |
| `samples/MessageBroker.ConfigSample` | JSON config file demo |

## Getting started

### 1. Define messages

Use `[MessageName]` to decouple the wire format name from the CLR type name:

```csharp
[MessageName("order.created")]
public sealed record OrderMessage(
    string OrderId = "",
    string Customer = "",
    decimal Amount = 0m,
    string Priority = "normal");

[MessageName("stock.price")]
public sealed record StockPriceMessage(
    string Ticker = "",
    decimal Price = 0m,
    DateTime AsOf = default);
```

### 2. Write a consumer

Implement `IConsume<T>`. One class can handle multiple message types:

```csharp
public sealed class TradingConsumer : IConsume<OrderMessage>, IConsume<StockPriceMessage>
{
    private readonly ILogger<TradingConsumer> _logger;

    public string Name => nameof(TradingConsumer);

    public TradingConsumer(ILogger<TradingConsumer> logger) => _logger = logger;

    public Task ConsumeAsync(OrderMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        _logger.LogInformation("Order {Id} from {Customer} — £{Amount}", msg.OrderId, msg.Customer, msg.Amount);
        return Task.CompletedTask;
    }

    public Task ConsumeAsync(StockPriceMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        _logger.LogInformation("{Ticker} = {Price:C} at {AsOf:HH:mm:ss}", msg.Ticker, msg.Price, msg.AsOf);
        return Task.CompletedTask;
    }
}
```

### 3. Register and run

```csharp
var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((_, services) =>
    {
        services.AddMessageBroker()
            .AddMemoryEndPoint("MemQueue")
            .AddFileEndPoint("FileOut")
            .AddConsumer<TradingConsumer>()
            .Build();
    })
    .Build();

var broker = host.Services.GetRequiredService<IMessageBroker>();

await host.StartAsync();
await broker.PostAsync("MemQueue", new OrderMessage { OrderId = "ORD-1", Customer = "Alice", Amount = 99m });
```

## Endpoints

### Memory

In-process bounded channel — useful for tests and internal routing:

```csharp
.AddMemoryEndPoint("MemQueue")            // default capacity 1000
.AddMemoryEndPoint("HighPriority", 100)   // custom capacity
```

### File

Watches a directory for incoming JSON files; writes outgoing messages to another:

```csharp
.AddFileEndPoint("FileIn")   // defaults: readPath="in", postPath="out"

.AddFileEndPoint("FileOut", new FileSettings
{
    ReadPath    = "processed",
    PostPath    = "out",
    PollInterval = TimeSpan.FromSeconds(2)
})
```

### RabbitMQ

Add a reference to `MessageBroker.RabbitMq` and use the extension method:

```csharp
using MessageBroker.RabbitMq;

services.AddMessageBroker()
    .AddRabbitMqEndPoint("RabbitIn", new RabbitMqSettings
    {
        HostName      = "localhost",
        ReadQueueName = "orders.in",
        WriteQueueName = "orders.out"
    })
    .AddConsumer<TradingConsumer>()
    .Build();
```

Start RabbitMQ with the provided Docker Compose file:

```bash
./setup-rabbitmq.ps1          # start and wait for healthy
./setup-rabbitmq.ps1 -Stop    # stop
./setup-rabbitmq.ps1 -Logs    # tail logs
```

## Routing

Route a message type to one or more destination endpoints. If a route matches, the message is forwarded to that endpoint. Consumer dispatch still runs unless explicitly suppressed.

```csharp
// All high-priority orders go to FileOut
broker.Route<OrderMessage>()
    .To("FileOut")
    .When(msg => msg.TryGetProperty("priority", out var p) && p.GetString() == "high")
    .Build();

// Route only messages that arrived from RabbitMQ
broker.Route<OrderMessage>()
    .To("Archive")
    .WhenFrom("RabbitIn")
    .Build();

// Route any message type older than 5 minutes
broker.Route()
    .To("DeadLetter")
    .WhenMessageIsOlderThan(TimeSpan.FromMinutes(5))
    .Build();

// Combine conditions
broker.Route<StockPriceMessage>()
    .To("AlertQueue")
    .WhenFrom("MarketFeed")
    .When(msg => msg.GetProperty("price").GetDecimal() > 1000m)
    .Build();
```

### Available conditions

| Method | Description |
|---|---|
| `.When(Func<JsonElement, bool>)` | Predicate on the message payload |
| `.WhenFrom(name)` | Source endpoint matches |
| `.WhenNotFrom(name)` | Source endpoint does not match |
| `.WhenMessageIsOlderThan(TimeSpan)` | Message age exceeds threshold |
| `.And(lhs, rhs)` | Both conditions must be true |
| `.Or(lhs, rhs)` | Either condition must be true |

## Scheduled actions

### Interval

```csharp
// Fire every 10 seconds, passing the broker as a parameter
broker.AddScheduledAction<IMessageBroker>(
    TimeSpan.FromSeconds(10),
    b => b.PostAsync("MemQueue", new StockPriceMessage
    {
        Ticker = "ACME",
        Price  = 42.00m,
        AsOf   = DateTime.UtcNow
    }).GetAwaiter().GetResult(),
    broker);
```

### Cron

Uses [Cronos](https://github.com/HangfireIO/Cronos) syntax with local timezone:

```csharp
// Every minute
broker.AddScheduledAction<IMessageBroker>(
    "* * * * *",
    b => b.PostAsync("MemQueue", new StockPriceMessage { Ticker = "CRON", Price = 42m, AsOf = DateTime.UtcNow })
           .GetAwaiter().GetResult(),
    broker);

// Mon–Fri at 17:00
broker.AddScheduledAction<IMessageBroker>(
    "0 17 * * 1-5",
    b => b.PostAsync("MemQueue", new DailyCloseMessage()).GetAwaiter().GetResult(),
    broker);
```

> **Note:** `AddScheduledAction` takes a synchronous `Action<T>`. Async broker calls inside must use `.GetAwaiter().GetResult()`.

Each action starts with a random jitter (100–2000 ms) to stagger multiple scheduled tasks.

## JSON configuration file

Declare endpoint topology in a file — consumers and routes are still registered in code:

**`queuesettings.json`**
```json
{
  "MessageBroker": {
    "Endpoints": [
      {
        "Name": "MemQueue",
        "Type": "Memory"
      },
      {
        "Name": "FileOut",
        "Type": "File",
        "Config": {
          "readPath": "processed",
          "postPath": "out"
        }
      },
      {
        "Name": "RabbitIn",
        "Type": "RabbitMq",
        "Config": {
          "hostName": "localhost",
          "port": 5672,
          "readQueueName": "orders.in",
          "writeQueueName": "orders.out"
        }
      }
    ]
  }
}
```

**`Program.cs`**
```csharp
services.AddMessageBroker()
    .LoadConfiguration("queuesettings.json")   // File + Memory endpoints registered automatically
    .WithRabbitMq()                            // from MessageBroker.RabbitMq — processes RabbitMq entries
    .AddConsumer<TradingConsumer>()
    .Build();
```

## Message envelope (wire format)

Every message is wrapped in a JSON envelope:

```json
{
  "id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "correlationId": "8f14e45f-...",
  "address": { "to": "FileOut", "from": "MemQueue" },
  "messageType": "order.created",
  "created": "2025-01-15T09:30:00Z",
  "traceParent": "00-4bf92f3...",
  "message": {
    "orderId": "ORD-001",
    "customer": "Alice",
    "amount": 299.99,
    "priority": "high"
  }
}
```

`messageType` uses the value from `[MessageName("...")]` if present, otherwise `Type.FullName`.

## Filters

Implement `IMessageFilter` to inspect or block messages before routing and dispatch:

```csharp
public sealed class AuditFilter : IMessageFilter
{
    public Task<bool> FilterAsync(IMessageContext context, CancellationToken ct = default)
    {
        Console.WriteLine($"[Audit] {context.MessageType} from {context.Address?.From}");
        return Task.FromResult(true);   // false = drop the message
    }
}

broker.AddFilter(new AuditFilter());
```

## Aggregator / Splitter

Split a large payload into chunks and reassemble on the other side:

```csharp
// Splitting
var splitter = host.Services.GetRequiredService<ISplitter>();
var parts = splitter.Split(largeBytes, new SizeBasedSplitCondition(maxBytes: 64_000));
foreach (var part in parts)
    await broker.PostAsync("MemQueue", part);

// Aggregation happens automatically inside ProcessAsync when all parts arrive.
// The reassembled message is dispatched as a normal message once GroupSize is met.
```

## Performance notes

| Technique | Detail |
|---|---|
| Compiled dispatch | `Expression.Lambda<>.Compile()` cached per message type — ~10× faster than `MethodInfo.Invoke` |
| RecyclableMemoryStream | All serialization uses a shared `RecyclableMemoryStreamManager` to reduce GC pressure |
| Lock-free collections | `ImmutableList`/`ImmutableDictionary` for routes and consumers — lock-free reads, atomic writes at config time |
| DI scope per message | `IServiceScopeFactory` creates a fresh scope for each consumer dispatch — supports `Scoped` lifetimes |

## Running the samples

```bash
# Fluent API demo (file + memory endpoints, scheduled actions, routing)
dotnet run --project samples/MessageBroker.Sample

# JSON config file demo
dotnet run --project samples/MessageBroker.ConfigSample
```

## Running tests

```bash
dotnet test
dotnet test --filter "FullyQualifiedName~SerializerTests"   # single class
```

## Design constraints

- .NET 10, no Windows-specific APIs
- `Microsoft.Extensions.DependencyInjection` only (no Autofac)
- `System.Text.Json` only (no Newtonsoft.Json)
- No XML / XSLT transforms — route conditions use `Func<JsonElement, bool>` predicates
- `RouteContext` is non-sealed with `virtual Evaluate()` — subclass for custom routing logic
