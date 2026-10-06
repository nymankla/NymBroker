# NymBroker

NymBroker is a .NET 10 message-processing framework based on [Enterprise Integration Patterns](https://www.enterpriseintegrationpatterns.com/). It decouples producers from handlers: applications post typed messages to endpoints, and the broker deserializes, filters, routes, and dispatches them to consumers or subscribers. Start with the in-process Memory endpoint, then add file, SQLite, PostgreSQL, SQL Server, RabbitMQ, or Azure Service Bus transports as your application grows.

```
Source Endpoint → [Wire Tap] → Deserialize → [TTL Check] → Filter → Router → Consumer / Destination Endpoint
                                                                                        ↓ on failure
                                                                              Dead Letter Channel
                                                                     ↕
                                                           Aggregator / Splitter
```

## Quickstart

You need the [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0). From the repository root, run the fluent API sample:

```bash
dotnet run --project samples/NymBroker.Sample
```

The sample uses in-process Memory and File endpoints, so no external broker or database is needed. It posts example orders, logs them through a consumer, and demonstrates routing and scheduled messages. Stop it with **Ctrl+C**.

To create a console app for the smallest useful broker setup, run these commands from the repository root:

```bash
dotnet new console --framework net10.0 --name Quickstart
dotnet add Quickstart/Quickstart.csproj reference NymBroker.Core/NymBroker.Core.csproj
dotnet add Quickstart/Quickstart.csproj package Microsoft.Extensions.Hosting --version 10.0.12
```

Replace `Quickstart/Program.cs` with:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((_, services) =>
    {
        services.AddNymBroker()
            .AddMemoryEndPoint("Orders")
            .AddConsumer<OrderConsumer>()
            .Build();
    })
    .Build();

var broker = host.Services.GetRequiredService<INymBroker>();
await host.StartAsync();
await broker.PostAsync("Orders", new Order("ORD-1"));
await host.WaitForShutdownAsync();

public sealed record Order(string Id);

public sealed class OrderConsumer : IConsume<Order>
{
    public Task ConsumeAsync(Order message, IMessageContext context, CancellationToken ct = default)
    {
        Console.WriteLine($"Received order {message.Id}");
        return Task.CompletedTask;
    }
}
```

`AddConsumer<T>()` registers the handler, `AddMemoryEndPoint()` creates a local queue, and `PostAsync()` sends a message to it. The host starts the broker listener; press **Ctrl+C** to stop.

## Core concepts

| Building block | Purpose |
|---|---|
| [Endpoints](#endpoints) | Named transport adapters that accept and/or deliver messages. Memory is useful for local work and tests; File, SQLite, PostgreSQL, SQL Server, RabbitMQ, and Azure Service Bus connect other systems. |
| [Routes](#routing) | Match message types and conditions, then forward messages to destination endpoints. |
| [Filters](#filters) | Inspect or modify a message before routing; return `null` to drop it. |
| [Consumers](#getting-started) | Implement `IConsume<T>` to handle messages of a particular type. Consumers are dispatched through dependency injection. |
| [Subscribers](#publish-subscribe-channel) | Implement `ISubscribe<T>` to receive copies published to a topic, independently of endpoint routing. |
| [Retries](NymBroker.Resilience/README.md) | Transport retries handle transient File and RabbitMQ failures; SQLite/PostgreSQL queue settings retry failed message processing. |

In short: a producer posts to an endpoint, the broker processes the message through filters and routes, then dispatches it to a consumer and/or destination endpoint. Publishing to a topic instead fans a copy out to its subscribers.

## Features

- **Multiple transports** — RabbitMQ, Azure Service Bus, SQLite, PostgreSQL, SQL Server, File system, and in-process Memory endpoint
- **Fluent routing API** — type-safe, composable route conditions
- **Typed consumers** — implement `IConsume<T>`, optionally handle multiple message types in one class
- **Publish-Subscribe Channel** — EIP pub/sub; declare topics with typed `ISubscribe<T>` subscribers or endpoint fan-out
- **Dead Letter Channel** — failed, expired and undecodable messages go to the transport's own dead-letter queue (RabbitMQ DLX, `Failed` rows in the SQL endpoints) or, for transports without one, to a configured dead-letter endpoint
- **Wire Tap** — copy every raw message to a secondary endpoint before processing; zero impact on normal flow
- **Idempotent Receiver** — deduplicate messages by ID using a TTL-based in-memory store; duplicate messages are silently dropped
- **Message Expiration (TTL)** — discard messages older than a configured age; optionally forward them to the dead-letter endpoint
- **Scheduled actions** — interval-based or Cron expression (via [Cronos](https://github.com/HangfireIO/Cronos))
- **JSON config file** — declare endpoint topology in `queuesettings.json`; consumers and routes stay in code
- **High performance** — compiled Expression dispatch, RecyclableMemoryStream, lock-free ImmutableCollections
- **Reliable delivery** — RabbitMQ uses manual ack/nack; no message is silently dropped on processing failure
- **Scoped consumers** — each message dispatch gets its own DI scope
- **Input transformers** — intercept raw bytes before envelope deserialization; convert any format (CSV, protobuf, plain text) into a `RawMessageContext` the broker can route and dispatch

## Observability

NymBroker uses the built-in `System.Diagnostics.Metrics` and `ActivitySource` APIs; the core package does not require an OpenTelemetry dependency. Configure your application's OpenTelemetry SDK (and exporters) to subscribe to the instrumentation names:

```csharp
services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics.AddMeter("NymBroker", "NymBroker.Resilience"))
    .WithTracing(tracing => tracing.AddSource("NymBroker"));
```

The broker emits these measurements:

| Instrument | Meter | Meaning |
|---|---|---|
| `nymbroker.messages.received` | `NymBroker` | Calls entering message processing, tagged with the source endpoint |
| `nymbroker.messages.routed` | `NymBroker` | Endpoint deliveries, tagged with `source`, `destination`, `message_type`, `via` (`route`/`topic`), optional `topic`, and `outcome` (`success`/`failure`) |
| `nymbroker.messages.consumed` | `NymBroker` | Consumer and topic-subscriber invocations, tagged with `source`, `message_type`, `consumer`, `kind` (`consumer`/`subscriber`), and `outcome` (`success`/`failure`) |
| `nymbroker.messages.failed` | `NymBroker` | Processing failures, including deserialization and consumer failures |
| `nymbroker.messages.dead_lettered` | `NymBroker` | Dead-lettered messages, tagged with `reason` (`DeadLetterReasons` or a transport reason), `source` endpoint and `mode` (`broker`: posted to the dead-letter endpoint; `native`: the broker returned `ProcessResult.DeadLetter`) |
| `nymbroker.message.processing.duration` | `NymBroker` | Processing latency in milliseconds, tagged with `outcome` (`success`/`failure`) and `result` (`completed`/`retry`/`dead_letter`, the `ProcessResult` returned to the endpoint) |
| `nymbroker.retries` | `NymBroker.Resilience` | Retry attempts made by `RetryPolicy` |

Each processing call is instrumented with a `nymbroker.process` consumer activity when a listener is attached. Once an envelope is decoded, the activity and logging scope carry the message ID, correlation ID, message type, and source endpoint. These identifiers let log aggregators and trace backends correlate broker-stage logs with a message. Activity context propagates across asynchronous processing; subscribe to `NymBroker` to export the spans.

For production dashboards and alerts, monitor message receive rate alongside routed and consumed rates by destination and consumer, the failure ratio per consumer, processing latency, retry rate, dead-letter and expired-message logs, and transport-specific queue depth/age and endpoint health. Roughly, received messages are accounted for by consumed + routed + dropped/expired/dead-lettered messages. The counters describe delivery attempts and handler invocations separately; a topic delivery may be both routed and consumed by subscribers, so this is not a strict sum. The core metrics describe broker processing and retries; queue depth and transport health should be collected from the configured endpoint or hosting platform.

## Solution layout

| Project | Purpose |
|---|---|
| `NymBroker.Core` | Framework core — no external transport dependency |
| `NymBroker.RabbitMq` | Optional RabbitMQ transport (add when needed) |
| [`NymBroker.Resilience`](NymBroker.Resilience/README.md) | Dependency-free retry policy (`RetryPolicy`) used by the File and RabbitMQ endpoints |
| `NymBroker.Sqlite` | Optional SQLite transport via Dapper (add when needed) |
| `NymBroker.Postgres` | Optional PostgreSQL transport via Npgsql |
| `NymBroker.SqlServer` | Optional SQL Server transport via Microsoft.Data.SqlClient |
| `NymBroker.AzureServiceBus` | Optional Azure Service Bus transport via Azure.Messaging.ServiceBus |
| `NymBroker.Tests` | xUnit tests |
| [`NymBroker.Sample`](samples/NymBroker.Sample) | Fluent API, Memory/File endpoints, routing, and scheduled actions |
| [`NymBroker.ConfigSample`](samples/NymBroker.ConfigSample) | Endpoint configuration from JSON |
| [`NymBroker.SqlSample`](samples/NymBroker.SqlSample) | SQLite queue and message processing |
| [`NymBroker.WebSample`](samples/NymBroker.WebSample) | ASP.NET Core REST API → SQLite queue → consumer |
| [`NymBroker.PostgresSample`](samples/NymBroker.PostgresSample) | PostgreSQL endpoint and queue processing |
| [`NymBroker.SqlServerSample`](samples/NymBroker.SqlServerSample) | SQL Server endpoint and queue processing |
| [`NymBroker.AzureServiceBusSample`](samples/NymBroker.AzureServiceBusSample) | Azure Service Bus endpoint: native dead-lettering and reading the dead-letter queue |
| [`NymBroker.ConsumerSample`](samples/NymBroker.ConsumerSample) | Long-running cross-process consumer (SQLite / PostgreSQL / RabbitMQ) |
| [`NymBroker.ProducerSample`](samples/NymBroker.ProducerSample) | Cross-process producer that posts to a shared queue |
| [`NymBroker.CsvSample`](samples/NymBroker.CsvSample) | Input transformer that converts raw CSV into typed messages |
| [`NymBroker.RabbitSample`](samples/NymBroker.RabbitSample) | RabbitMQ transport |
| [`NymBroker.RoutingSample`](samples/NymBroker.RoutingSample) | Endpoint routing and publish/subscribe |
| [`NymBroker.Benchmarks`](samples/NymBroker.Benchmarks) | Throughput and allocation benchmarks |

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

Need a transport that isn't listed here? See [Writing an Endpoint](docs/writing-an-endpoint.md) for the endpoint contract, samples for sink, push and pull transports, registration and testing.

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
| `UseNativeDeadLetter` | `true` | Settle failures in the table (retry up to `MaxRetryCount`, mark undecodable/expired messages `Failed` at once). `false` sends failures to the broker's dead-letter endpoint instead and marks the row Completed |

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

- **Drains back to back:** while messages are waiting, batches are claimed one after another. When the queue is empty, the endpoint waits for a `NOTIFY` (sent by `PostAsync` when `UseNotifications` is on) or `PollInterval`, whichever comes first.
- **One round trip and one commit per batch:** the results of a batch are written in the same `NpgsqlBatch` (one implicit transaction) that claims the next one.
- **Stays fast as the table grows:** a partial index covers only Pending and InProgress rows, so a claim reads just the batch it takes, however large the backlog or the number of completed rows.
- **Safe late writes:** results are only written if the row is still InProgress with the same attempt number, so a poller whose lease expired can't overwrite a row another poller has claimed since.
- **Clean shutdown:** `StopListeningAsync` waits for the poll loop and writes the results of messages already handled; claimed but unhandled messages are redelivered when their lease expires.

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
    payload          BYTEA       NOT NULL
);
-- Only Pending/InProgress rows are indexed, so claiming stays cheap as the table grows.
CREATE INDEX ix_nymbroker_messages_active
    ON nymbroker_messages(created_at_utc, queue_id)
    WHERE status IN (0, 1);
```

Tables created by NymBroker 0.1.4 or earlier had two full indexes on `status` instead. With `AutoCreateTable = true`, the endpoint adds the partial index and drops those two on first connect.

**`PostgresSettings` properties:**

| Property | Default | Description |
|---|---|---|
| `ConnectionString` | `Host=localhost;Database=nymbroker;Username=postgres;Password=postgres` | PostgreSQL connection string |
| `TableName` | `nymbroker_messages` | Table to read/write |
| `BatchSize` | `10` | Max rows read per poll cycle |
| `AutoCreateTable` | `true` | Create table + indexes on first connect |
| `PollInterval` | `100 ms` | How long to wait after a poll that found no messages (cut short by a `NOTIFY` when `UseNotifications` is on); while messages are waiting, batches are claimed back to back |
| `LeaseTimeout` | `5 min` | How long a claimed message stays leased before it can be reclaimed |
| `MaxRetryCount` | `5` | Number of failed attempts before a message is marked `Failed` |
| `UseNativeDeadLetter` | `true` | Settle failures in the table (retry up to `MaxRetryCount`, mark undecodable/expired messages `Failed` at once). `false` sends failures to the broker's dead-letter endpoint instead and marks the row Completed |
| `UseNotifications` | `true` | `PostAsync` sends a `NOTIFY` so idle listeners wake immediately instead of waiting out `PollInterval` |

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

### SQL Server

Add a reference to `NymBroker.SqlServer` and use the extension method:

```csharp
using NymBroker.SqlServer;

services.AddNymBroker()
    .AddSqlServerEndPoint("SqlServerQueue", new SqlServerSettings
    {
        ConnectionString = "Server=localhost,1433;Database=nymbroker;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True",
        TableName        = "dbo.orders",
        BatchSize        = 10,
        AutoCreateTable  = true,
        LeaseTimeout     = TimeSpan.FromMinutes(5),
        MaxRetryCount    = 5
    })
    .AddConsumer<OrderConsumer>()
    .Build();
```

It has the same `Pending -> InProgress -> Completed/Failed` lifecycle, leases and retry handling as the PostgreSQL endpoint, implemented with SQL Server patterns:

- **Competing consumers:** rows are claimed with `UPDLOCK, READPAST`, so several application instances can poll the same table without processing a message twice.
- **One round trip and one commit per batch:** the results of a batch (Completed / back to Pending / Failed) are written in the same T-SQL batch and transaction that claims the next one.
- **Stays fast as the table grows:** a filtered index covers only Pending and InProgress rows, so claiming doesn't slow down as completed rows pile up.
- **Safe late writes:** results are only written if the row is still InProgress with the same attempt number, so a poller whose lease expired can't overwrite a row another poller has claimed since.
- **Clean shutdown:** stopping the listener writes the results of messages already handled; claimed but unhandled messages are redelivered when their lease expires.

SQL Server has no lightweight `LISTEN/NOTIFY`, so when the queue is empty the endpoint polls every `PollInterval`; while messages are waiting it claims batches back to back. Requires SQL Server 2016 or later (it uses `OPENJSON`).

Start a local SQL Server with `./scripts/setup-sqlserver.ps1`. It runs SQL Server 2022 in Docker on `localhost,1433` and creates the `nymbroker` database; `-Stop` and `-Logs` work as for the other scripts.

**Schema** (auto-created when `AutoCreateTable = true`):

```sql
CREATE TABLE [dbo].[nymbroker_messages] (
    queue_id         BIGINT IDENTITY(1,1) NOT NULL,
    message_id       UNIQUEIDENTIFIER NOT NULL,
    status           INT              NOT NULL DEFAULT 0 CHECK (status IN (0, 1, 2, 3)),
    created_at_utc   DATETIME2(7)     NOT NULL DEFAULT SYSUTCDATETIME(),
    locked_until_utc DATETIME2(7)     NULL,
    completed_at_utc DATETIME2(7)     NULL,
    failed_at_utc    DATETIME2(7)     NULL,
    attempt_count    INT              NOT NULL DEFAULT 0,
    last_error       NVARCHAR(MAX)    NULL,
    payload          VARBINARY(MAX)   NOT NULL,
    CONSTRAINT [pk_dbo_nymbroker_messages] PRIMARY KEY CLUSTERED (queue_id)
);
-- Only Pending/InProgress rows are indexed, so claiming stays cheap as completed rows accumulate.
CREATE INDEX [ix_dbo_nymbroker_messages_active]
    ON [dbo].[nymbroker_messages](queue_id) INCLUDE (status, locked_until_utc)
    WHERE status IN (0, 1);
```

**`SqlServerSettings` properties:**

| Property | Default | Description |
|---|---|---|
| `ConnectionString` | `Server=localhost,1433;Database=nymbroker;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True` | SQL Server connection string (the default matches `setup-sqlserver.ps1`) |
| `TableName` | `dbo.nymbroker_messages` | Table to read/write; `schema.table` or `table` |
| `BatchSize` | `10` | Max rows claimed per poll cycle |
| `AutoCreateTable` | `true` | Create table + indexes on first connect |
| `PollInterval` | `100 ms` | Delay after a poll that found no messages; while messages are waiting, batches are claimed back to back |
| `LeaseTimeout` | `5 min` | How long a claimed message stays leased before it can be reclaimed |
| `MaxRetryCount` | `5` | Number of failed attempts before a message is marked `Failed` |
| `UseNativeDeadLetter` | `true` | Settle failures in the table (retry up to `MaxRetryCount`, mark undecodable/expired messages `Failed` at once). `false` sends failures to the broker's dead-letter endpoint instead and marks the row Completed |

From a JSON config file (call `.WithSqlServer()` after `.LoadConfiguration()`; the type name is case-insensitive):

```json
{
  "NymBroker": {
    "Endpoints": [
      {
        "Name": "SqlServerQueue",
        "Type": "SqlServer",
        "Config": {
          "connectionString": "Server=localhost,1433;Database=nymbroker;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True",
          "tableName": "dbo.orders",
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
    .WithSqlServer()
    .AddConsumer<OrderConsumer>()
    .Build();
```

### Azure Service Bus

Add a reference to `NymBroker.AzureServiceBus` and use the extension method:

```csharp
using NymBroker.AzureServiceBus;

services.AddNymBroker()
    .AddAzureServiceBusEndPoint("Orders", new AzureServiceBusSettings
    {
        ConnectionString = "<namespace connection string>",
        QueueName        = "orders"
    })
    .AddConsumer<OrderConsumer>()
    .Build();

// Or with Azure AD (reference Azure.Identity in your app):
    .AddAzureServiceBusEndPoint("Orders", new AzureServiceBusSettings
    {
        FullyQualifiedNamespace = "myns.servicebus.windows.net",
        Credential              = new DefaultAzureCredential(),
        TopicName               = "orders",        // send to a topic ...
        SubscriptionName        = "billing"        // ... receive from one of its subscriptions
    })
```

Messages are received with a `ServiceBusProcessor` in peek-lock mode and settled by the broker's result:

| Outcome | Settlement |
|---|---|
| Processed | `Complete` |
| A consumer or topic subscriber fails | `Abandon`: Service Bus redelivers it, and moves it to the dead-letter queue with reason `MaxDeliveryCountExceeded` once the entity's `MaxDeliveryCount` is reached |
| Can never succeed (undecodable, expired, unknown compression) | `DeadLetter` at once, with the reason (`DeadLetterReasons`) and description |

Read a dead-letter queue with a second endpoint on the same entity and `ReadDeadLetterQueue = true`, for example to repair or replay messages; a consumer that returns normally removes the message from the dead-letter queue. The client, sender and processor are long-lived; transient faults are retried by the SDK.

A message can be at most 256 KB on the Standard tier (up to 100 MB on Premium), so pass `splitThresholdBytes` (for example `200_000`) to `PostAsync` for larger messages.

**`AzureServiceBusSettings` properties:**

| Property | Default | Description |
|---|---|---|
| `ConnectionString` | — | Namespace or emulator connection string. Use this **or** `FullyQualifiedNamespace` + `Credential` |
| `FullyQualifiedNamespace` | — | e.g. `myns.servicebus.windows.net` |
| `Credential` | — | `TokenCredential` (e.g. `DefaultAzureCredential`); code only, not read from config files |
| `QueueName` | — | Queue to send to and receive from. Use this **or** `TopicName` |
| `TopicName` / `SubscriptionName` | — | Send to the topic; receive from the subscription |
| `ReadDeadLetterQueue` | `false` | Receive from the entity's dead-letter queue |
| `MaxConcurrentCalls` | `1` | Messages handled in parallel; above 1 gives up ordering |
| `PrefetchCount` | `0` | Messages fetched ahead |
| `MaxAutoLockRenewalDuration` | `5 min` | How long a message lock is renewed while the handler runs |
| `UseNativeDeadLetter` | `true` | Use the entity's dead-letter queue. `false` sends failures to the broker's dead-letter endpoint instead and completes the message |

Invalid combinations (both or neither of the connection options or entity names) throw when the endpoint is registered.

From a JSON config file (call `.WithAzureServiceBus()` after `.LoadConfiguration()`; the type name is case-insensitive):

```json
{
  "NymBroker": {
    "Endpoints": [
      { "Name": "Orders", "Type": "AzureServiceBus", "Config": { "connectionString": "...", "queueName": "orders" } }
    ]
  }
}
```

**Local development:** `./scripts/setup-servicebus.ps1` starts the [Service Bus emulator](https://learn.microsoft.com/azure/service-bus-messaging/overview-emulator) in Docker (it stores its state in the `sqlserver` service, which starts with it). Entities are declared in `scripts/servicebus/Config.json`. Its AMQP port is mapped to **5673** so it can run next to RabbitMQ:

```text
Endpoint=sb://localhost:5673;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;
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

Messages are consumed with `autoAck: false`. A message is acked after successful processing. When a consumer fails it is nacked with `requeue: true` once; if it fails again after redelivery it is treated as a poison message and nacked with `requeue: false` (dead-lettered when the queue has a DLX; disable via `RejectRedeliveredFailures = false`, which requeues indefinitely). Messages that can never succeed (undecodable, expired) are nacked with `requeue: false` immediately. Set `UseNativeDeadLetter = false` to send failures to the broker's dead-letter endpoint instead (the message is then acked). The endpoint reconnects automatically on connection loss using the built-in `NymBroker.Resilience` retry policy.

Start RabbitMQ with the provided Docker Compose file:

```bash
./scripts/setup-rabbitmq.ps1          # start and wait for healthy
./scripts/setup-rabbitmq.ps1 -Stop    # stop
./scripts/setup-rabbitmq.ps1 -Logs    # tail logs
```

## Batch posting

Post or publish many messages in one call. Each message still gets its **own envelope** (own id, routing, TTL, dead-letter reason) and receivers get them **one by one**, exactly as if they had been posted separately. Consumers, subscribers, filters and dead-lettering need no changes, and one bad message is retried or dead-lettered on its own. The gain is on the sending side: the endpoint can send the whole batch in one round trip or transaction.

```csharp
// One endpoint call for all of them; order is preserved.
await broker.PostBatchAsync("Orders", orders);

// Same CorrelationId on every envelope, so the batch can be traced.
await broker.PostBatchAsync("Orders", orders, correlationId: Guid.NewGuid());

// Large messages are still split per message; the parts go in the same batch.
await broker.PostBatchAsync("Orders", orders, splitThresholdBytes: 200_000);

// Publish: by type, each message goes through routes, topics and consumers in turn.
await broker.PublishBatchAsync(events);

// Publish to a named topic: one batch per subscriber endpoint; ISubscribe<T> subscribers receive them one by one.
await broker.PublishBatchAsync("orders.events", events);
```

They are separate methods rather than `PostAsync` overloads on purpose: `PostAsync(endpoint, list)` would bind to `PostAsync<T>` with `T = List<…>` and post the whole list as **one** message.

How each transport sends a batch:

| Endpoint | Batch send | Atomic? |
|---|---|---|
| SQL Server | one `INSERT … SELECT FROM OPENJSON` | yes |
| PostgreSQL | one `INSERT … SELECT FROM unnest`, one `NOTIFY` per batch | yes |
| SQLite | one transaction | yes |
| Azure Service Bus | as few `ServiceBusMessageBatch`es as their size allows | per Service Bus batch (a large post is split into several) |
| RabbitMQ, File, Memory | one message at a time (the default) | no |

If a non-atomic batch fails partway, `PostBatchAsync` throws and some messages may already have been sent; use `AddIdempotentReceiver()` if you retry. An empty batch does nothing; a `null` element throws `ArgumentException` before anything is sent; unknown and read-only endpoints throw as `PostAsync` does.

Custom endpoints can override `IEndPoint.PostBatchAsync(IReadOnlyList<byte[]>)`; the default posts the messages one at a time.

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

Route a message type to one or more destination endpoints. If a route matches, the message is forwarded to that endpoint. If a route matches (or a topic matches, via endpoint fan-out or `ISubscribe<T>` subscribers), the message is **not** consumed by registered consumers. Consumer dispatch only runs when the message matches no route and no topic.

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

Each add-on package processes its own entries: `.WithRabbitMq()` (`RabbitMq`), `.WithSql()` (`Sql`), `.WithPostgres()` (`Postgres`) and `.WithSqlServer()` (`SqlServer`). `Type` is an open, case-insensitive string, so a file can also declare endpoint types from your own packages. An entry whose `With*()` is never called is ignored.

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

## Input transformers

An `IInputTransformer` intercepts the raw bytes arriving at an endpoint **before** the default JSON-envelope deserialization runs. Use it when the producer sends data in a format that is not a NymBroker JSON envelope — CSV, plain-text, protobuf, fixed-width, etc.

```csharp
public interface IInputTransformer
{
    /// <summary>
    /// Return null to drop the message silently.
    /// </summary>
    RawMessageContext? Transform(ReadOnlySpan<byte> input, string? sourceEndpoint);
}
```

`RawMessageContext` is the internal form the broker uses after normal deserialization. Populate `MessageType` to match a registered `[MessageName]` and set `RawMessage` to a `JsonElement` that holds the typed payload:

```csharp
public sealed class CsvOrderTransformer : IInputTransformer
{
    public RawMessageContext? Transform(ReadOnlySpan<byte> input, string? sourceEndpoint)
    {
        var parts = Encoding.UTF8.GetString(input).Trim().Split(',');
        if (parts.Length != 4) return null;   // drop malformed lines

        if (!decimal.TryParse(parts[2].Trim(), NumberStyles.Any, CultureInfo.InvariantCulture, out var amount))
            return null;

        var payload = JsonSerializer.SerializeToElement(new
        {
            orderId  = parts[0].Trim(),
            customer = parts[1].Trim(),
            amount,
            priority = parts[3].Trim()
        });

        return new RawMessageContext
        {
            Id            = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            MessageType   = "order.created",
            Created       = DateTime.UtcNow,
            RawMessage    = payload
        };
    }
}
```

### Registration

Register a transformer for a specific endpoint, or globally (applied when no per-endpoint transformer matches):

```csharp
services.AddNymBroker()
    .AddMemoryEndPoint("CsvInbox")
    .AddInputTransformer<CsvOrderTransformer>("CsvInbox")   // per-endpoint
    .AddConsumer<OrderConsumer>()
    .Build();
```

```csharp
services.AddNymBroker()
    .AddMemoryEndPoint("CsvInbox")
    .AddInputTransformer<CsvOrderTransformer>()             // global fallback
    .AddConsumer<OrderConsumer>()
    .Build();
```

### Posting raw bytes

Use `broker.PostAsync(string, Stream)` to push non-envelope bytes into the endpoint. Cast to `Stream` explicitly — otherwise C# overload resolution picks the generic `PostAsync<T>` and tries to serialize the `MemoryStream` as a message:

```csharp
var csv   = "ORD-001,Alice,499.99,high";
var bytes = Encoding.UTF8.GetBytes(csv);
await broker.PostAsync("CsvInbox", (Stream)new MemoryStream(bytes));
```

The transformer runs inside `ProcessAsync`; returning `null` silently drops the message with no logging.

## Reliability patterns

### Dead Letter Channel

Failed messages are set aside so they can be inspected or reprocessed without blocking the main flow. Where they go depends on the endpoint the message came from.

**Endpoints with their own dead-letter queue** (`UsesNativeDeadLetter`): RabbitMQ, Azure Service Bus and the SQLite, PostgreSQL and SQL Server endpoints, with `UseNativeDeadLetter = true` (the default). The broker tells the endpoint what to do through the `ProcessResult` it returns, and the transport does the dead-lettering:

| Failure | Result | RabbitMQ | Azure Service Bus | SQL endpoints |
|---|---|---|---|---|
| A consumer or topic subscriber throws | `Retry` | requeued once, then rejected to the queue's dead-letter exchange | abandoned; dead-lettered (`MaxDeliveryCountExceeded`) after the entity's `MaxDeliveryCount` | back to `Pending`, `Failed` after `MaxRetryCount` |
| Undecodable bytes, expired (TTL), unknown compression | `DeadLetter` with a reason | rejected to the dead-letter exchange at once | dead-lettered at once with the reason and description | `Failed` at once, reason in the error column |
| A route's destination fails | `Retry` | as above | as above | as above |

**All other endpoints** (Memory, File, custom endpoints without a dead-letter queue, or `UseNativeDeadLetter = false`): the broker posts the message to the endpoint named by `WithDeadLetterEndpoint` and the source message is completed. This covers consumer and topic failures, expired messages, and undecodable messages (which were dropped before 0.2.0). The posted bytes are the original envelope with an added `deadLetter` block that records why (see below).

```csharp
services.AddNymBroker()
    .AddMemoryEndPoint("Main")
    .AddMemoryEndPoint("DeadLetters", mode: EndpointMode.WriteOnly)
    .AddConsumer<OrderConsumer>()
    .WithDeadLetterEndpoint("DeadLetters")
    .Build();
```

- `StartAsync` throws `InvalidOperationException` if the dead-letter endpoint is `ReadOnly`.
- Make the dead-letter endpoint `WriteOnly` unless you mean to consume from it in the same broker: a listening dead-letter endpoint feeds its messages straight back into processing, and a consumer that keeps failing would loop.
- Every dead-lettering is logged at `Warning` with its reason (`DeadLetterReasons`) and counted in `nymbroker.messages.dead_lettered`.

**The `deadLetter` block.** A dead-lettered envelope gets an optional `deadLetter` object; normal messages are unchanged on the wire. A message that is dead-lettered again gets the block *replaced* by the latest failure.

```json
{ "id": "…", "messageType": "orders.created", "message": { … },
  "deadLetter": { "reason": "ConsumerFailed", "description": "InvalidOperationException: stock not available",
                  "exceptionType": "System.InvalidOperationException", "sourceEndpoint": "OrdersIn",
                  "deadLetteredAt": "2026-10-05T12:00:00Z", "deliveryCount": null } }
```

Consumers read it as `context.DeadLetter` (`DeadLetterInfo`), and it survives routing. The description is the exception message capped at 4 096 characters (no stack trace). `deliveryCount` is filled only when the transport knows it (Service Bus). `DeadLetterEnvelope.Annotate(raw, info)` adds the block to any bytes; the Azure Service Bus endpoint uses it when `ReadDeadLetterQueue = true`, so a Service Bus DLQ reader sees the same block (reason `MaxDeliveryCountExceeded`, etc.).

Bytes that are not a JSON object (not JSON, invalid UTF-8, an array) are wrapped in a new envelope of type `nymbroker.undecodable` — `UndecodableMessage { PayloadBase64, PayloadText }`, with `PayloadText` set only for valid UTF-8 — so a dead-letter consumer can handle them with `IConsume<UndecodableMessage>`.

Replay example — consume from the dead-letter endpoint and re-post:

```csharp
public sealed class ReplayConsumer(INymBroker broker) : IConsume<Order>
{
    public async Task ConsumeAsync(Order message, IMessageContext context, CancellationToken ct = default)
    {
        if (context.DeadLetter?.Reason == DeadLetterReasons.ConsumerFailed)
            await broker.PostAsync("Main", message, ct);   // a fresh envelope; a failure again dead-letters it with a new block
    }
}
```

### Wire Tap

Copy every raw message to one or more secondary endpoints before any processing occurs. The tap sees all messages — including those that will later be filtered, expired, or dead-lettered. Tap failures are logged and do not affect normal message flow.

```csharp
services.AddNymBroker()
    .AddMemoryEndPoint("Main")
    .AddMemoryEndPoint("AuditLog")
    .AddConsumer<OrderConsumer>()
    .AddWireTap("AuditLog")
    .Build();
```

Multiple taps are supported — call `.AddWireTap()` once per endpoint. `StartAsync` throws if a tap endpoint is `ReadOnly`.

### Idempotent Receiver

Drop duplicate messages using an in-memory TTL store keyed on the message `id` field. A duplicate within the TTL window is silently discarded; an entry whose TTL has expired allows the same ID to be processed again.

```csharp
services.AddNymBroker()
    .AddMemoryEndPoint("Main")
    .AddConsumer<OrderConsumer>()
    .AddIdempotentReceiver(TimeSpan.FromHours(1))   // omit TimeSpan to use the default 24-hour TTL
    .Build();
```

The store is registered as `IIdempotencyStore` in the DI container and applied as a pipeline filter before routing and consumer dispatch.

### Message Expiration (TTL)

Discard messages older than a configured age. Expiry is checked after deserialization (so `created` is available) but before filters and routing. Expired messages are forwarded to the dead-letter endpoint when one is configured.

```csharp
services.AddNymBroker()
    .AddMemoryEndPoint("Main")
    .AddMemoryEndPoint("DeadLetters")
    .AddConsumer<OrderConsumer>()
    .WithDeadLetterEndpoint("DeadLetters")
    .DiscardMessagesOlderThan(TimeSpan.FromMinutes(30))
    .Build();
```

The age is calculated from the `created` field in the message envelope using `DateTime.UtcNow`. Messages without an explicit `created` timestamp default to the current time and will never expire.

### Combining patterns

All four patterns compose cleanly:

```csharp
services.AddNymBroker()
    .AddMemoryEndPoint("Main")
    .AddMemoryEndPoint("AuditLog")
    .AddMemoryEndPoint("DeadLetters")
    .AddConsumer<OrderConsumer>()
    .AddWireTap("AuditLog")                                 // copy every message
    .AddIdempotentReceiver(TimeSpan.FromHours(2))           // drop duplicates
    .DiscardMessagesOlderThan(TimeSpan.FromMinutes(15))     // discard stale messages
    .WithDeadLetterEndpoint("DeadLetters")                  // catch failures + expired
    .Build();
```

**Pipeline order:**

1. Wire tap — raw bytes forwarded before any other processing
2. Transformer / deserialization
3. TTL check — expired → dead letter
4. Filter loop — idempotency filter runs here
5. Routing, topic fan-out, consumer dispatch — consumer failure → retried by the transport, or dead letter (see above)

## Aggregator / Splitter

Split a large payload into chunks and reassemble on the other side:

```csharp
// Splitting
var splitter = host.Services.GetRequiredService<ISplitter>();
var parts = splitter.Split(largeBytes, new DefaultSplitCondition(maxChunkSizeBytes: 64_000));
foreach (var part in parts)
    await broker.PostAsync("MemQueue", part);

// Aggregation happens automatically inside ProcessAsync when all parts arrive.
// The reassembled message is dispatched as a normal message once GroupSize is met.
```

### Auto-split on post

`PostAsync` can split large messages for you — pass `splitThresholdBytes` and any serialized
envelope larger than that gets transparently broken into `SplitMessage` parts before being posted;
messages under the threshold are posted unchanged:

```csharp
await broker.PostAsync("MemQueue", largeOrder, ct: default, splitThresholdBytes: 64_000);
```

The receiving side needs no special handling — `ProcessAsync` reassembles the parts and dispatches
the original message once all of them have arrived, exactly as with manual splitting above.

Splitting Base64-encodes each chunk, which inflates the data by ~33% before it's even chunked.
By default `PostAsync` offsets that: it compresses the envelope (`ICompressor`, Brotli by default)
before splitting, whenever compression actually shrinks it — for typical JSON/text payloads this
more than cancels out the Base64 overhead. Pass `compress: false` to disable it (e.g. for payloads
that are already compressed or binary, where compressing again wastes CPU for no benefit — the
framework also detects and skips this case automatically). Compressed parts carry
`SplitMessage.Compression` (e.g. `"brotli"`) so the receiving side knows to decompress after
reassembly; if compression alone brings the payload under the threshold, it still travels as a
single-part `SplitMessage` group, since compressed bytes aren't valid envelope JSON on their own.

```csharp
// Disable compression (e.g. payload is already compressed/binary):
await broker.PostAsync("MemQueue", largeImage, ct: default, splitThresholdBytes: 64_000, compress: false);
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

# PostgreSQL endpoint demo (start Postgres first with ./scripts/setup-postgres.ps1)
dotnet run --project samples/NymBroker.PostgresSample

# SQL Server endpoint demo (start SQL Server first with ./scripts/setup-sqlserver.ps1)
dotnet run --project samples/NymBroker.SqlServerSample

# Azure Service Bus endpoint demo (start the emulator first with ./scripts/setup-servicebus.ps1)
dotnet run --project samples/NymBroker.AzureServiceBusSample

# CSV input transformer demo — posts raw CSV lines, broker converts and dispatches as typed messages
dotnet run --project samples/NymBroker.CsvSample
dotnet run --project samples/NymBroker.CsvSample -- "ORD-99,Zara,12.50,low"   # single custom line

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
| `postgres` | PostgreSQL on `localhost` — `./scripts/setup-postgres.ps1` |
| `rabbitmq` | RabbitMQ on `localhost` — `./scripts/setup-rabbitmq.ps1` |

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
| **File – direct** | `FileLoop` | 100 | Messages are written as JSON files to `bench-in/`, picked up by `FileSystemWatcher`, deserialized, and dispatched. Exercises the full file I/O path including the IOException retry policy and `.processed` rename. Lower count because disk I/O dominates. |
| **SQL – direct** | `SqlBench` | 1 000 | Messages are inserted into an in-memory SQLite database via Dapper, then claimed and dispatched by the endpoint's internal poll loop (`BatchSize=100`, `PollInterval=0`). Measures the overhead of the optimistic UPDATE claim and async Dapper round trips. |
| **Split+Compress – direct** | `Memory` | 200 | Each message carries a ~276 KB highly compressible payload and is posted with `splitThresholdBytes: 16 384` and `compress: true`. `PostAsync` compresses the envelope with `BrotliCompressor` before handing it to `ISplitter`, so fewer/smaller `SplitMessage` parts are posted; `AggregatorImpl` reassembles and decompresses them on arrival. Measures the split+compress+reassemble round trip end to end. |
| **Split – no compress** | `Memory` | 200 | Same payload and threshold as above but `compress: false`, so the envelope is split into Base64-chunked `SplitMessage` parts without compression. Isolates the cost/benefit of compression by comparing directly against **Split+Compress – direct**. |
| **Postgres – direct** | `PgBench` | 1 000 | Messages are inserted into a real PostgreSQL table, then claimed using `FOR UPDATE SKIP LOCKED` and dispatched (`BatchSize=50`, `PollInterval=0`). Skipped automatically when PostgreSQL is not reachable. Measures the overhead of TCP round trips and the CTE-based atomic claim. |
| **SqlServer – direct** | `MssqlBench` | 1 000 | Messages are inserted into a real SQL Server table (`dbo.nymbroker_bench`, dropped before each run), then claimed with `UPDLOCK, READPAST` and dispatched (`BatchSize=50`, `PollInterval=0`). Skipped automatically when SQL Server is not reachable. Start it with `./scripts/setup-sqlserver.ps1`. |
| **ServiceBus – direct** | `SbBench` | 1 000 | Messages are sent to the `nymbroker.bench` queue of the Service Bus emulator (drained before the run) and received with a `ServiceBusProcessor` (`MaxConcurrentCalls=1`, `PrefetchCount=100`), each completed after processing. Skipped automatically when the emulator or queue is not reachable. Start it with `./scripts/setup-servicebus.ps1`. |
| **Postgres / SqlServer / ServiceBus – batch/100** | same as `… – direct` | 1 000 | The same messages posted with `PostBatchAsync` in batches of 100 instead of one `PostAsync` each. Shows the send-side gain of batch posting. |

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

`FileLoop` uses the same directory for reading and writing (`bench-in`), so posted files are immediately visible to the `FileSystemWatcher`. `PubSubDest` is the fan-out target used by the **PubSub – endpoint** scenario. The `Postgres – direct`, `SqlServer – direct` and `ServiceBus – direct` scenarios add their endpoints directly in code (not via the settings file) and are skipped if their database or the emulator is unreachable.

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
Split+Compress – direct             359      557 ms     57     57     57    3.2 MB/msg
Split – no compress                 259      772 ms     93     59     41    5.3 MB/msg
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
- **Split+Compress vs Split – no compress** isolate the compression step: with the same 16 KB threshold and ~276 KB compressible payload, compression cuts the part count roughly in half (fewer `SplitMessage` posts and reassembly steps), which is why the compressed variant is both faster and allocates less despite paying the Brotli compress/decompress cost. Allocation is dominated by the Base64-encoded chunk strings, not the framework dispatch path — this is the one scenario where megabyte-scale allocations are expected. For incompressible payloads (already-compressed binary, encrypted blobs), expect the two scenarios to converge since `PostAsync` skips compression whenever it doesn't shrink the payload.
- **Postgres** runs against a local PostgreSQL instance over TCP (`BatchSize=50`, `PollInterval=0`). The scenario posts one message at a time, so it is bound by the INSERT commit (~1.7 ms each on Docker Desktop; ~420 msg/s in the same run as the SqlServer note below). Consuming is faster: each batch is one round trip and one commit, and a backlog drains at ~9 000 msg/s (`BatchSize=50`), or ~3 000–5 000 msg/s with the default settings.
- **SqlServer** posts one message at a time, so it is bound by the INSERT: every commit waits for a transaction-log flush, which takes ~3 ms on Docker Desktop's virtual disk. Against the `setup-sqlserver.ps1` container on Windows it measured ~215 msg/s, with Postgres at ~400 msg/s in the same run (neither is in the results above, which come from an earlier run on different storage). Consuming is much faster: draining a backlog runs at ~4 500–6 000 msg/s with `BatchSize=50`, because each batch is one round trip and one commit. Concurrent producers also get more throughput, since SQL Server groups their commits into shared log flushes (~1 900 msg/s with 16 producers). With real server storage, expect higher numbers across the board. Don't enable `DELAYED_DURABILITY` for a real queue to speed up inserts, because it can lose committed messages on a crash.
- **ServiceBus** measured ~40 msg/s against the local emulator. That is the emulator's latency, not the endpoint: with the SDK alone, each send took ~15 ms and receiving with one message at a time (a complete per message) reached ~37 msg/s. Receiving scales with `MaxConcurrentCalls` (~300 msg/s with 8, at the cost of ordering), and batched sends are far cheaper than one at a time. Expect very different numbers against a real namespace, where latency depends on region and tier.
- **batch/100** (same run on the local containers): Postgres 544 → 9 009 msg/s and SQL Server 321 → 5 434 msg/s, with allocations down from ~20 KB to ~6–7 KB per message — the insert becomes one statement and one commit per 100 messages. ServiceBus stays at ~38 msg/s end to end: the send gets cheaper, but the scenario is bound by receiving one message at a time with a complete per message (raise `MaxConcurrentCalls` for that).

## Running tests

```bash
dotnet test
dotnet test --project NymBroker.Tests -- --filter-class "*SerializerTests"   # single class
```

The PostgreSQL, SQL Server and Azure Service Bus integration tests are skipped unless their connection-string variables are set. To run them, start the services with `./scripts/setup-postgres.ps1`, `./scripts/setup-sqlserver.ps1` and `./scripts/setup-servicebus.ps1`, then:

```powershell
$env:NYMBROKER_POSTGRES_CS   = "Host=localhost;Database=nymbroker;Username=postgres;Password=postgres"
$env:NYMBROKER_SQLSERVER_CS  = "Server=localhost,1433;Database=nymbroker;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True"
$env:NYMBROKER_SERVICEBUS_CS = "Endpoint=sb://localhost:5673;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
dotnet test
```

## Design constraints

- .NET 10, no Windows-specific APIs
- `Microsoft.Extensions.DependencyInjection` only (no Autofac)
- `System.Text.Json` only (no Newtonsoft.Json)
- No XML / XSLT transforms — route conditions use `Func<JsonElement, bool>` predicates
- `RouteContext` is non-sealed with `virtual Evaluate()` — subclass for custom routing logic
