# NymBroker

A .NET 10 enterprise message processing framework following [Enterprise Integration Patterns](https://www.enterpriseintegrationpatterns.com/). Messages flow as JSON envelopes through a configurable pipeline of endpoints, filters, routers, and consumers.

```
Source Endpoint → Deserialize → Filter → Router → Consumer / Destination Endpoint
                                              ↕
                                    Aggregator / Splitter
```

## Features

- **Multiple transports** — RabbitMQ, SQLite, PostgreSQL, File system, and in-process Memory endpoint
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
| `NymBroker.Core` | Framework core — no external transport dependency |
| `NymBroker.RabbitMq` | Optional RabbitMQ transport (add when needed) |
| `NymBroker.Sqlite` | Optional SQLite transport via Dapper (add when needed) |
| `NymBroker.Postgres` | Optional PostgreSQL transport via Npgsql |
| `NymBroker.Tests` | xUnit tests |
| `samples/NymBroker.Sample` | Fluent API demo |
| `samples/NymBroker.ConfigSample` | JSON config file demo |
| `samples/NymBroker.SqlSample` | SQLite endpoint demo |
| `samples/NymBroker.WebSample` | ASP.NET Core minimal API demo — REST POST → SQLite queue → consumer |
| `samples/NymBroker.PostgresSample` | PostgreSQL endpoint demo — posts orders before start, broker reads from DB |
| `samples/NymBroker.ConsumerSample` | Cross-process consumer — listens on a shared queue (SQLite / Postgres / RabbitMQ) |
| `samples/NymBroker.ProducerSample` | Cross-process producer — posts orders to a shared queue then exits |
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

In-process bounded `Channel<byte[]>` — zero I/O, useful for internal routing and tests:

```csharp
.AddMemoryEndPoint("MemQueue")            // default capacity 1 000
.AddMemoryEndPoint("HighPriority", 100)   // custom capacity
```

`PostAsync` blocks when the channel is full (backpressure). `EnqueueAsync` is also available for direct string injection without serialization overhead:

```csharp
var ep = host.Services.GetRequiredKeyedService<IEndPoint>("MemQueue") as MemoryQueueEndPoint;
await ep!.EnqueueAsync("""{"orderId":"ORD-1"}""");
```

From a JSON config file (`Memory` endpoints are processed automatically by `LoadConfiguration`):

```json
{ "Name": "MemQueue", "Type": "Memory" }
```

### File

Watches a directory for incoming JSON files and writes outgoing messages to another directory. Incoming files are renamed to `.processed` after a successful read:

```csharp
.AddFileEndPoint("FileIn")   // defaults: ReadPath="in", PostPath="out"

.AddFileEndPoint("FileOut", new FileSettings
{
    ReadPath       = "orders-in",
    PostPath       = "orders-out",
    SearchPattern  = "*.json",
    PollInterval   = TimeSpan.FromSeconds(5),
    IsAbsolutePath = false        // paths are relative to the working directory
})
```

**`FileSettings` properties:**

| Property | Default | Description |
|---|---|---|
| `ReadPath` | `in` | Directory to watch for incoming files |
| `PostPath` | `out` | Directory to write outgoing files |
| `SearchPattern` | `*.json` | File glob pattern for the watcher |
| `PollInterval` | `5 s` | How often to scan for files that were missed by the watcher |
| `IsAbsolutePath` | `false` | When `true`, `ReadPath`/`PostPath` are treated as absolute paths |

From a JSON config file:

```json
{
  "Name": "FileOut",
  "Type": "File",
  "Config": {
    "readPath": "orders-in",
    "postPath": "orders-out",
    "searchPattern": "*.json",
    "pollInterval": "00:00:05"
  }
}
```

### SQL (SQLite)

Add a reference to `NymBroker.Sql` and use the extension method:

```csharp
using NymBroker.Sql;

services.AddNymBroker()
    .AddSqliteEndPoint("SqlQueue", new SqliteSettings
    {
        ConnectionString = "Data Source=messages.db",
        TableName        = "NymBrokerMessages",
        BatchSize        = 10,
        AutoCreateTable  = true,  // creates table + indexes on first use
        LeaseTimeout     = TimeSpan.FromMinutes(5),
        MaxRetryCount    = 5
    })
    .AddConsumer<OrderConsumer>()
    .Build();
```

Messages written via `PostAsync` are stored as `Pending` rows. The broker claims them by moving them to `InProgress`, setting a lease (`LockedUntilUtc`) and incrementing `AttemptCount`. Successful processing moves them to `Completed`; failures are returned to `Pending` until `MaxRetryCount` is reached, after which they are marked `Failed`. Expired leases are reclaimable, so multiple application instances can safely poll the same database.

**Schema** (auto-created when `AutoCreateTable = true`):

```sql
CREATE TABLE NymBrokerMessages (
    QueueId        INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    MessageId      TEXT    NOT NULL UNIQUE,
    Status         INTEGER NOT NULL DEFAULT 0 CHECK (Status IN (0, 1, 2, 3)),
    CreatedAtUtc   INTEGER NOT NULL DEFAULT (unixepoch()),
    LockedUntilUtc INTEGER NULL,
    CompletedAtUtc INTEGER NULL,
    FailedAtUtc    INTEGER NULL,
    AttemptCount   INTEGER NOT NULL DEFAULT 0,
    LastError      TEXT NULL,
    Payload        TEXT NOT NULL
);
```

**`SqliteSettings` properties:**

| Property | Default | Description |
|---|---|---|
| `ConnectionString` | `Data Source=messages.db` | SQLite connection string |
| `TableName` | `NymBrokerMessages` | Table to read/write |
| `BatchSize` | `10` | Max rows read per poll cycle |
| `AutoCreateTable` | `true` | Create table + indexes on first connect |
| `PollInterval` | `100 ms` | Delay between poll cycles; `TimeSpan.Zero` = poll immediately after a full batch |
| `LeaseTimeout` | `5 min` | How long a claimed message stays leased before it can be reclaimed |
| `MaxRetryCount` | `5` | Number of failed attempts before a message is marked `Failed` |

From a JSON config file (call `.WithSql()` after `.LoadConfiguration()`):

```json
{
  "NymBroker": {
    "Endpoints": [
      {
        "Name": "SqlQueue",
        "Type": "Sql",
        "Config": {
          "connectionString": "Data Source=messages.db",
          "tableName": "Orders",
          "batchSize": 25
        }
      }
    ]
  }
}
```

```csharp
services.AddNymBroker()
    .LoadConfiguration("queuesettings.json")
    .WithSql()
    .AddConsumer<OrderConsumer>()
    .Build();
```

### PostgreSQL

Add a reference to `NymBroker.Postgres` and use the extension method:

```csharp
using NymBroker.Postgres;

services.AddNymBroker()
    .AddPostgresEndPoint("PgQueue", new PostgresSettings
    {
        ConnectionString = "Host=localhost;Database=nymbroker;Username=postgres;Password=postgres",
        TableName        = "nymbroker_messages",
        BatchSize        = 10,
        AutoCreateTable  = true,
        LeaseTimeout     = TimeSpan.FromMinutes(5),
        MaxRetryCount    = 5
    })
    .AddConsumer<OrderConsumer>()
    .Build();
```

Messages use the same lifecycle as the SQLite endpoint (`Pending -> InProgress -> Completed/Failed`), but claiming is implemented with PostgreSQL row locking using `FOR UPDATE SKIP LOCKED`. That allows concurrent consumers across multiple application instances without a process-wide lock.

**Schema** (auto-created when `AutoCreateTable = true`):

```sql
CREATE TABLE nymbroker_messages (
    queue_id         BIGSERIAL PRIMARY KEY,
    message_id       UUID        NOT NULL UNIQUE,
    status           INTEGER     NOT NULL DEFAULT 0 CHECK (status IN (0, 1, 2, 3)),
    created_at_utc   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_until_utc TIMESTAMPTZ NULL,
    completed_at_utc TIMESTAMPTZ NULL,
    failed_at_utc    TIMESTAMPTZ NULL,
    attempt_count    INTEGER     NOT NULL DEFAULT 0,
    last_error       TEXT        NULL,
    payload          TEXT        NOT NULL
);
```

**`PostgresSettings` properties:**

| Property | Default | Description |
|---|---|---|
| `ConnectionString` | `Host=localhost;Database=nymbroker;Username=postgres;Password=postgres` | PostgreSQL connection string |
| `TableName` | `nymbroker_messages` | Table to read/write |
| `BatchSize` | `10` | Max rows read per poll cycle |
| `AutoCreateTable` | `true` | Create table + indexes on first connect |
| `PollInterval` | `100 ms` | Delay between poll cycles; `TimeSpan.Zero` = poll immediately after a full batch |
| `LeaseTimeout` | `5 min` | How long a claimed message stays leased before it can be reclaimed |
| `MaxRetryCount` | `5` | Number of failed attempts before a message is marked `Failed` |

From a JSON config file (call `.WithPostgres()` after `.LoadConfiguration()`):

```json
{
  "NymBroker": {
    "Endpoints": [
      {
        "Name": "PgQueue",
        "Type": "Postgres",
        "Config": {
          "connectionString": "Host=localhost;Database=nymbroker;Username=postgres;Password=postgres",
          "tableName": "orders",
          "batchSize": 25
        }
      }
    ]
  }
}
```

```csharp
services.AddNymBroker()
    .LoadConfiguration("queuesettings.json")
    .WithPostgres()
    .AddConsumer<OrderConsumer>()
    .Build();
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

## Endpoint modes

Every endpoint has an `EndpointMode` that controls whether it can read, write, or both:

| Mode | Description |
|---|---|
| `ReadWrite` (default) | Endpoint participates in both sending and receiving |
| `ReadOnly` | Listener is started; `PostAsync` and routing to this endpoint throw at runtime |
| `WriteOnly` | No listener is started; the endpoint is only used for posting messages |

Pass the mode as the last argument to any `Add*EndPoint` method:

```csharp
// ReadWrite is the default — no argument needed
.AddMemoryEndPoint("Inbox")

// ReadOnly — only a consumer/listener, never a routing destination
.AddSqliteEndPoint("AuditLog", settings, EndpointMode.ReadOnly)

// WriteOnly — posts messages and exits; no poll loop is started
.AddPostgresEndPoint("OutboxQueue", settings, EndpointMode.WriteOnly)
```

`StartAsync` logs each endpoint and its mode:

```
info: NymBroker.Core.Impl.NymBrokerImpl[0]
      Endpoint 'OutboxQueue' registered (WriteOnly)
info: NymBroker.Core.Impl.NymBrokerImpl[0]
      Endpoint 'OutboxQueue' is write-only — listener not started
```

On shutdown, each endpoint whose listener was started emits a matching stop line:

```
info: NymBroker.Core.Impl.NymBrokerImpl[0]
      Stopped listening on endpoint 'Inbox'
```

**Validation at startup:** `StartAsync` throws `InvalidOperationException` if a route or topic targets a `ReadOnly` endpoint — misconfiguration is caught before the first message arrives.

### Producer / consumer pattern

`WriteOnly` is designed for CLI producers that post a batch and exit immediately — no poll loop means the process doesn't hang:

```csharp
// producer process — posts and exits
services.AddNymBroker()
    .AddSqliteEndPoint("Queue", new SqliteSettings { ConnectionString = "...", AutoCreateTable = true },
                       EndpointMode.WriteOnly)
    .Build();

await host.StartAsync();
await broker.PostAsync("Queue", new OrderMessage(...));
await host.StopAsync();   // returns immediately; no listener to cancel
```

```csharp
// consumer process — runs until Ctrl+C
services.AddNymBroker()
    .AddSqliteEndPoint("Queue", new SqliteSettings { ConnectionString = "...", AutoCreateTable = true })
    .AddConsumer<OrderConsumer>()
    .Build();

await host.RunAsync();
```

The `NymBroker.ProducerSample` and `NymBroker.ConsumerSample` projects demonstrate this pattern for SQLite, PostgreSQL, and RabbitMQ.

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
    public IMessageContext? Filter(IMessageContext context)
    {
        Console.WriteLine($"[Audit] {context.MessageType} from {context.Address?.From}");
        return context;   // return null to drop the message
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
| Byte[] transport pipeline | Both directions use `byte[]`: inbound handlers deliver `byte[]` to `ProcessAsync` (deserialized via `ReadOnlySpan<byte>`); outbound `IEndPoint.PostAsync` also accepts `byte[]` directly — eliminates stream allocation and copy on every posted message |
| Zero-copy `PublishAsync` | `PublishAsync<T>` extracts bytes from the `RecyclableMemoryStream` buffer rather than round-tripping through `StreamReader` → `string` → re-encode — ~67% fewer allocations on pub/sub paths |
| Lazy deserialization in pub/sub | The message object is deserialized at most once per `ProcessAsync` call even when multiple topics match; endpoints receive the already-serialized bytes directly |
| Per-subscriber error isolation | A failing `ISubscribe<T>` handler logs the error and continues fan-out to remaining subscribers rather than aborting the batch |

## Running the samples

```bash
# Fluent API demo (file + memory endpoints, scheduled actions, routing)
dotnet run --project samples/NymBroker.Sample

# JSON config file demo
dotnet run --project samples/NymBroker.ConfigSample

# SQLite endpoint demo (posts orders before start, broker reads them from DB)
dotnet run --project samples/NymBroker.SqlSample

# ASP.NET Core web API demo (REST POST → SQLite queue → consumer)
dotnet run --project samples/NymBroker.WebSample
# then open http://localhost:5000 for the Scalar API explorer

# PostgreSQL endpoint demo (start Postgres first with ./setup-postgres.ps1)
dotnet run --project samples/NymBroker.PostgresSample

# Cross-process producer/consumer pair (SQLite by default; pass --transport postgres or --transport rabbitmq)
# Terminal 1 — start the consumer first:
dotnet run --project samples/NymBroker.ConsumerSample -- --transport sqlite
# Terminal 2 — post messages:
dotnet run --project samples/NymBroker.ProducerSample -- --transport sqlite --count 5
```

### Producer / consumer pair

Run in two separate terminals. Start the consumer first, then post messages with the producer.

**Terminal 1 — consumer (runs until Ctrl+C):**
```bash
dotnet run --project samples/NymBroker.ConsumerSample -- --transport sqlite
```

**Terminal 2 — producer (posts N messages and exits):**
```bash
dotnet run --project samples/NymBroker.ProducerSample -- --transport sqlite --count 10
```

`--count` defaults to 3. The producer registers its endpoint as `WriteOnly` so no listener is started. Both samples default to `--transport sqlite` using `consumer-sample.db` in the working directory.

| `--transport` | Prerequisite |
|---|---|
| `sqlite` (default) | none |
| `postgres` | PostgreSQL on `localhost` — `./setup-postgres.ps1` |
| `rabbitmq` | RabbitMQ on `localhost` — `./setup-rabbitmq.ps1` |

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
| **SQL – direct** | `SqlBench` | 1 000 | Messages are inserted into an in-memory SQLite database via Dapper, then claimed and dispatched by the endpoint's internal poll loop (`BatchSize=100`, `PollInterval=0`). Measures the overhead of the optimistic UPDATE claim and async Dapper round trips. |
| **Postgres – direct** | `PgBench` | 1 000 | Messages are inserted into a real PostgreSQL table, then claimed using `FOR UPDATE SKIP LOCKED` and dispatched (`BatchSize=50`, `PollInterval=0`). Skipped automatically when PostgreSQL is not reachable. Measures the overhead of TCP round trips and the CTE-based atomic claim. |

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

`FileLoop` uses the same directory for reading and writing (`bench-in`), so posted files are immediately visible to the `FileSystemWatcher`. `PubSubDest` is the fan-out target used by the **PubSub – endpoint** scenario. The `Postgres – direct` scenario adds its endpoint directly in code (not via the settings file) and is skipped if PostgreSQL is unreachable.

### Completion tracking

`BenchmarkConsumer` calls `CompletionTracker.Signal()` on every message. `CompletionTracker` uses `Interlocked.Decrement` on a countdown from the target count; when it reaches zero it signals a `TaskCompletionSource`. The benchmark waits on that task (30 s timeout) before stopping the clock. This ensures elapsed time covers the full end-to-end latency, not just the post loop.

### Indicative results (Windows 11, .NET 10, Ryzen 7 — allocation figures are the stable signal; throughput varies with GC scheduling)

```
Scenario                        Msg/sec     Elapsed   Gen0   Gen1   Gen2     Alloc/msg
──────────────────────────────────────────────────────────────────────────────────────
Memory – direct                  97 087      515 ms     14      0      0    3.4 KB/msg
Memory – routed                  77 041      649 ms     24      0      0    5.9 KB/msg
Memory – filtered               152 439      328 ms     13      0      0    3.4 KB/msg
PubSub – 1 sub                   99 206      504 ms     10      0      0    2.6 KB/msg
PubSub – 3 subs                 108 695      460 ms     10      0      0    2.7 KB/msg
PubSub – endpoint               176 678      283 ms     19      0      0    4.9 KB/msg
File   – direct                     460      217 ms      0      0      0   92.8 KB/msg
SQL    – direct                     980    1 020 ms      1      0      0   12.4 KB/msg
Postgres – direct                 1 218      821 ms      1      0      0   14.2 KB/msg
```

Run `dotnet run -c Release --project samples/NymBroker.Benchmarks` for numbers on your hardware.

Notes on the numbers:
- **Memory – direct** throughput shows high run-to-run variance due to GC scheduling; the allocation figure (2.7 KB/msg) is the stable signal. The dominant cost is `IServiceScopeFactory.CreateAsyncScope()` per dispatch.
- **Memory – routed** is slower than direct because each message is serialized a second time to re-post to `MemoryRouted`, doubling the Gen0 collections.
- **Memory – filtered** can appear faster than direct because the DI scope and consumer dispatch happen on a hot path with an already-JIT-compiled filter chain; the delta is within run-to-run noise.
- **PubSub – 1 sub** adds one compiled-lambda call and a `GetRequiredKeyedService` lookup per message on top of the direct path. ~2.6 KB/msg allocation; the zero-copy `PublishAsync` path avoids the `StreamReader` → string round-trip.
- **PubSub – 3 subs** throughput measured as messages-posted/elapsed; since each message signals three times the total signal count is `3 × 50 000`. Expect roughly `1/N_subs` throughput relative to 1 sub due to sequential fan-out within a topic.
- **PubSub – endpoint** exercises the endpoint-based fan-out path: the topic posts bytes directly to `PubSubDest` via `IEndPoint.PostAsync(byte[])`, where the broker picks it up and dispatches to `BenchmarkConsumer`. The byte[] outbound path removes the intermediate stream copy, which is why this scenario sees the largest throughput gain over older builds.
- **File** allocation is ~93 KB/msg. The write side uses `File.WriteAllBytesAsync`; the read side deserializes via `File.ReadAllBytesAsync`. The dominant cost is file system round-trips and the `.processed` rename.
- **SQL** runs against an in-memory SQLite database (`BatchSize=100`, `PollInterval=0`). Each message costs one INSERT plus a SELECT and UPDATE (optimistic claim). ~1 000 msg/s is the ceiling for single-connection `:memory:` SQLite; a file-backed database will be lower.
- **Postgres** runs against a local PostgreSQL instance over TCP (`BatchSize=50`, `PollInterval=0`). Each message costs one INSERT (post) plus a CTE `FOR UPDATE SKIP LOCKED` claim plus a finalize UPDATE — three round trips. ~1 200 msg/s reflects TCP latency; throughput scales with batch size and connection pooling in multi-instance deployments.

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
