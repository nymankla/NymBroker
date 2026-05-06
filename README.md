# NymBroker

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
- **Publish-Subscribe Channel** — EIP pub/sub; declare topics with typed `ISubscribe<T>` subscribers or endpoint fan-out
- **Scheduled actions** — interval-based or Cron expression (via [Cronos](https://github.com/HangfireIO/Cronos))
- **JSON config file** — declare endpoint topology in `queuesettings.json`; consumers and routes stay in code
- **High performance** — compiled Expression dispatch, RecyclableMemoryStream, lock-free ImmutableCollections
- **Reliable delivery** — RabbitMQ uses manual ack/nack; no message is silently dropped on processing failure
- **Scoped consumers** — each message dispatch gets its own DI scope

## Solution layout

| Project | Purpose |
|---|---|
| `NymBroker.Core` | Framework core — no RabbitMQ dependency |
| `NymBroker.RabbitMq` | Optional RabbitMQ transport (add when needed) |
| `NymBroker.Tests` | xUnit tests |
| `samples/NymBroker.Sample` | Fluent API demo |
| `samples/NymBroker.ConfigSample` | JSON config file demo |
| `samples/NymBroker.Benchmarks` | Throughput and allocation benchmark |

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
        services.AddNymBroker()
            .AddMemoryEndPoint("MemQueue")
            .AddFileEndPoint("FileOut")
            .AddConsumer<TradingConsumer>()
            .Build();
    })
    .Build();

var broker = host.Services.GetRequiredService<INymBroker>();

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

Add a reference to `NymBroker.RabbitMq` and use the extension method:

```csharp
using NymBroker.RabbitMq;

services.AddNymBroker()
    .AddRabbitMqEndPoint("RabbitIn", new RabbitMqSettings
    {
        HostName      = "localhost",
        ReadQueueName = "orders.in",
        WriteQueueName = "orders.out"
    })
    .AddConsumer<TradingConsumer>()
    .Build();
```

Messages are consumed with `autoAck: false`. A message is acked after successful processing or nacked with `requeue: true` on failure, so no message is lost if the handler throws. The endpoint reconnects automatically on connection loss using Polly.

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

## Publish-Subscribe Channel

Topics implement the [Publish-Subscribe Channel](https://www.enterpriseintegrationpatterns.com/patterns/messaging/PublishSubscribeChannel.html) EIP pattern. A single message fans out to every subscriber simultaneously; each receives its own independent copy.

### Subscribers

Implement `ISubscribe<T>` — the pub/sub counterpart to `IConsume<T>`:

```csharp
public sealed class AuditSubscriber : ISubscribe<OrderMessage>
{
    public Task ReceiveAsync(OrderMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        Console.WriteLine($"[Audit] Order {msg.OrderId}");
        return Task.CompletedTask;
    }
}

public sealed class AnalyticsSubscriber : ISubscribe<OrderMessage>
{
    public Task ReceiveAsync(OrderMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        // record to analytics store...
        return Task.CompletedTask;
    }
}
```

### Registering topics

```csharp
services.AddNymBroker()
    .AddMemoryEndPoint("Orders")
    .AddConsumer<OrderConsumer>()
    .AddTopic<OrderMessage>("orders.events")
        .SubscribeWith<AuditSubscriber>()
        .SubscribeWith<AnalyticsSubscriber>()
        .Build()
    .Build();
```

### Publishing

```csharp
// Named publish — routes directly to the topic, bypasses the full ProcessAsync pipeline
await broker.PublishAsync("orders.events", new OrderMessage { OrderId = "ORD-1" });

// Implicit — any OrderMessage arriving at any endpoint triggers fan-out automatically
await broker.PostAsync("Orders", new OrderMessage { OrderId = "ORD-2" });
```

### Fan-out to an endpoint

Forward a copy to a destination endpoint instead of (or alongside) in-process subscribers:

```csharp
.AddTopic<OrderMessage>("orders.events")
    .SubscribeTo("FileOut")             // post a copy to this endpoint
    .SubscribeWith<AuditSubscriber>()   // also dispatch in-process
    .Build()
```

When fanning out to an endpoint, prevent the copy from re-triggering the same topic by adding a source guard:

```csharp
.AddTopic<OrderMessage>("orders.events")
    .When(new NotFromRouteCondition("FileOut"))
    .SubscribeTo("FileOut")
    .Build()
```

### Conditional topics

```csharp
.AddTopic<OrderMessage>("high-priority")
    .When(msg => msg.TryGetProperty("priority", out var p) && p.GetString() == "high")
    .SubscribeWith<PriorityHandler>()
    .Build()
```

## Scheduled actions

### Interval

```csharp
// Fire every 10 seconds, passing the broker as a parameter
broker.AddScheduledAction<INymBroker>(
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
broker.AddScheduledAction<INymBroker>(
    "* * * * *",
    b => b.PostAsync("MemQueue", new StockPriceMessage { Ticker = "CRON", Price = 42m, AsOf = DateTime.UtcNow })
           .GetAwaiter().GetResult(),
    broker);

// Mon–Fri at 17:00
broker.AddScheduledAction<INymBroker>(
    "0 17 * * 1-5",
    b => b.PostAsync("MemQueue", new DailyCloseMessage()).GetAwaiter().GetResult(),
    broker);
```

> **Note:** `AddScheduledAction` takes a synchronous `Action<T>`. Async broker calls inside must use `.GetAwaiter().GetResult()`.

## JSON configuration file

Declare endpoint topology in a file — consumers and routes are still registered in code:

**`queuesettings.json`**
```json
{
  "NymBroker": {
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
services.AddNymBroker()
    .LoadConfiguration("queuesettings.json")   // File + Memory endpoints registered automatically
    .WithRabbitMq()                            // from NymBroker.RabbitMq — processes RabbitMq entries
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
| Compiled dispatch | `Expression.Lambda<>.Compile()` cached per message type — ~10× faster than `MethodInfo.Invoke`; used for both `IConsume<T>` and `ISubscribe<T>` dispatch |
| RecyclableMemoryStream | All serialization uses a shared `RecyclableMemoryStreamManager` to reduce GC pressure |
| Lock-free collections | `ImmutableList`/`ImmutableDictionary` for routes, consumers, and topic registrations — lock-free reads, `ImmutableInterlocked.Update` CAS-loop for atomic multi-key writes at config time |
| DI scope per message | `IServiceScopeFactory` creates a fresh scope for each consumer or subscriber dispatch — supports `Scoped` lifetimes |
| Lazy deserialization in pub/sub | The message object is deserialized at most once per `ProcessAsync` call even when multiple topics match; endpoints receive the already-serialized JSON string directly |
| Per-subscriber error isolation | A failing `ISubscribe<T>` handler logs the error and continues fan-out to remaining subscribers rather than aborting the batch |

## Running the samples

```bash
# Fluent API demo (file + memory endpoints, scheduled actions, routing)
dotnet run --project samples/NymBroker.Sample

# JSON config file demo
dotnet run --project samples/NymBroker.ConfigSample
```

## Benchmarks

`samples/NymBroker.Benchmarks` is a standalone throughput and allocation benchmark. Run it with:

```bash
dotnet run --project samples/NymBroker.Benchmarks
```

### What it measures

The benchmark posts a batch of messages through the full framework pipeline — serialization, deserialization, routing, and consumer dispatch — and reports:

| Column | Meaning |
|---|---|
| **Msg/sec** | End-to-end throughput (messages posted ÷ elapsed time) |
| **Elapsed** | Wall-clock time for the whole batch |
| **Gen0/1/2** | GC generation collections triggered during the run |
| **Alloc/msg** | Managed bytes allocated per message (from `GC.GetTotalAllocatedBytes`) |

A warmup pass runs first to JIT the hot paths before measurements begin. GC is forced between scenarios to give each a clean baseline.

### Scenarios

| Scenario | Endpoint | Count | Description |
|---|---|---|---|
| **Memory – direct** | `Memory` (in-process channel) | 50 000 | Message is posted, deserialized, and dispatched to `BenchmarkConsumer` with no routing. Represents the fastest possible path through the broker. |
| **Memory – routed** | `Memory` → `MemoryRouted` | 50 000 | A route forwards every message from `Memory` to a second `MemoryRouted` endpoint, where the consumer picks it up. Exercises route evaluation, re-serialization, and a second dispatch cycle. |
| **Memory – filtered** | `Memory` (with filter) | 50 000 | A no-op `PassthroughFilter` is inserted into the pipeline. Isolates the overhead of filter chain evaluation. |
| **PubSub – 1 sub** | `Memory` | 50 000 | A single `ISubscribe<T>` subscriber registered on a topic. Baseline pub/sub cost: one compiled-lambda dispatch per message inside a DI scope, no endpoint fan-out. |
| **PubSub – 3 subs** | `Memory` | 50 000 × 3 | Three subscribers on the same topic. Fan-out is sequential within the topic; each message must signal three times before it counts as complete. Measures per-subscriber overhead and shows how throughput scales with subscriber count. |
| **PubSub – endpoint** | `Memory` → `PubSubDest` | 50 000 | Topic fans out to a second `PubSubDest` memory endpoint; the consumer on that endpoint signals. `NotFromRouteCondition` prevents the copy from re-triggering the topic. Exercises the endpoint-based fan-out path. |
| **File – direct** | `FileLoop` | 100 | Messages are written as JSON files to `bench-in/`, picked up by `FileSystemWatcher`, deserialized, and dispatched. Exercises the full file I/O path including Polly retry and `.processed` rename. Lower count because disk I/O dominates. |

### Configuration

Endpoint topology is declared in `benchmarksettings.json` (loaded via `LoadConfiguration`):

```json
{
  "NymBroker": {
    "Endpoints": [
      { "Name": "Memory",       "Type": "Memory" },
      { "Name": "MemoryRouted", "Type": "Memory" },
      { "Name": "PubSubDest",   "Type": "Memory" },
      {
        "Name": "FileLoop",
        "Type": "File",
        "Config": { "readPath": "bench-in", "postPath": "bench-in" }
      }
    ]
  }
}
```

`FileLoop` uses the same directory for reading and writing (`bench-in`), so posted files are immediately visible to the `FileSystemWatcher`. `PubSubDest` is the fan-out target used by the **PubSub – endpoint** scenario.

### Completion tracking

`BenchmarkConsumer` calls `CompletionTracker.Signal()` on every message. `CompletionTracker` uses `Interlocked.Decrement` on a countdown from the target count; when it reaches zero it signals a `TaskCompletionSource`. The benchmark waits on that task (30 s timeout) before stopping the clock. This ensures elapsed time covers the full end-to-end latency, not just the post loop.

### Indicative results (Windows 11, .NET 10, Ryzen 7)

```
Scenario                      Msg/sec     Elapsed   Gen0   Gen1   Gen2     Alloc/msg
───────────────────────────────────────────────────────────────────────────────────
Memory – direct                56 000      889 ms     33      0      0    7.9 KB/msg
Memory – routed                33 000    1 525 ms     66     26      0   14.9 KB/msg
Memory – filtered             114 000      439 ms     33     12      0    7.9 KB/msg
PubSub – 1 sub                     —          —      —      —      —           —
PubSub – 3 subs                    —          —      —      —      —           —
PubSub – endpoint                  —          —      —      —      —           —
File   – direct                   300      325 ms      1      0      0  145.2 KB/msg
```

Run `dotnet run --project samples/NymBroker.Benchmarks` for current pub/sub numbers on your hardware.

Notes on the numbers:
- **Memory – routed** is slower than direct because each message is serialized a second time to re-post to `MemoryRouted`, doubling the Gen0 collections.
- **Memory – filtered** can appear faster than direct because the DI scope and consumer dispatch happen on a hot path with an already-JIT-compiled filter chain; the delta is within run-to-run noise.
- **PubSub – 1 sub** adds one compiled-lambda call and a `GetRequiredKeyedService` lookup per message on top of the direct path.
- **PubSub – 3 subs** throughput measured as messages-posted/elapsed; since each message signals three times the total signal count is `3 × 50 000`. Expect roughly `1/(N_subs)` throughput relative to 1 sub due to sequential fan-out within a topic.
- **PubSub – endpoint** exercises the endpoint-based fan-out path: the topic serializes the message context and posts it to `PubSubDest`, where the broker picks it up and dispatches to `BenchmarkConsumer`.
- **File** allocation per message is high because `StreamReader.ReadToEndAsync` allocates a string for each file; this is inherent to file-based transport.

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
