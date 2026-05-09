# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A .NET 10 enterprise message processing framework following EIP (Enterprise Integration Patterns). Messages flow as JSON envelopes through a configurable pipeline:

```
Source Endpoint → Deserialize → Filter → Router → Consumer / Destination Endpoint
                                              ↕
                                    Aggregator / Splitter
```

Endpoints: **RabbitMQ**, **SQLite**, **PostgreSQL**, **File**, **Memory** (in-process / tests).

## Commands

Run from the solution root (`e:\utv\POC\NymBroker`):

```bash
dotnet build
dotnet test
dotnet test --filter "FullyQualifiedName~SerializerTests"   # single test class
dotnet run --project samples/NymBroker.Sample            # fluent API demo
dotnet run --project samples/NymBroker.ConfigSample      # JSON config demo
dotnet run --project samples/NymBroker.SqlSample         # SQLite endpoint demo
dotnet run --project samples/NymBroker.Benchmarks        # throughput benchmark

# RabbitMQ (Docker Desktop required)
./setup-rabbitmq.ps1          # start + wait for healthy
./setup-rabbitmq.ps1 -Stop    # stop
./setup-rabbitmq.ps1 -Logs    # tail logs
```

## Architecture

### Solution Layout

| Project | Role |
|---|---|
| `NymBroker.Core` | Framework core — endpoints, serializer, routing, broker engine, factory. No transport dependency. |
| `NymBroker.RabbitMq` | Optional add-on — `RabbitMqEndPoint`, `RabbitMqSettings`, `AddRabbitMqEndPoint`/`WithRabbitMq`. |
| `NymBroker.Sqlite` | Optional add-on — `SqliteEndPoint`, `SqliteSettings`, `AddSqliteEndPoint`/`WithSql`. Uses Dapper + `Microsoft.Data.Sqlite`. |
| `NymBroker.Postgres` | Optional add-on — `PostgresEndPoint`, `PostgresSettings`, `AddPostgresEndPoint`/`WithPostgres`. Uses Npgsql. |
| `NymBroker.Tests` | xUnit tests — uses Memory and SQLite `:memory:` endpoints; no RabbitMQ/Postgres/file I/O. |
| `samples/NymBroker.Sample` | Runnable demo with file + memory endpoints, scheduled actions, routing |
| `samples/NymBroker.ConfigSample` | Demo using `queuesettings.json` for endpoint configuration |
| `samples/NymBroker.SqlSample` | SQLite endpoint demo — posts orders, broker claims and dispatches |
| `samples/NymBroker.Benchmarks` | Throughput + allocation benchmark — Memory, File, and SQLite scenarios |

### Key Abstractions

```
IEndPoint                 ← all transports
  IEndPointPoll           ← pull-based: ReadAsync yields current items then completes
  IEndPointEventDriven    ← push-based: StartListeningAsync + handler callback

IMessageContext<T>        ← typed envelope (Id, CorrelationId, Address, MessageType, Created)
RawMessageContext          ← internal deserialized form; holds JsonElement RawMessage for deferred typing

INymBroker            ← engine facade (see below)
IConsume<T>               ← consumer contract; implement + register via AddConsumer<T>()
IRouteBuilder<T>          ← fluent route definition (see Routing section)
IRouteCondition           ← composable predicate evaluated on (IMessageContext, JsonElement)
```

`NymBrokerImpl.StartAsync` only starts `IEndPointEventDriven` endpoints. `IEndPointPoll`-only endpoints are never auto-driven by the broker engine. `SqliteEndPoint` and `PostgresEndPoint` implement **both** interfaces — their internal poll loop runs on `Task.Run` and calls `ProcessAsync` on each claimed message.

### Message Envelope (JSON wire format)

```json
{
  "id": "guid",
  "correlationId": "guid",
  "address": { "to": "FileOut", "from": "RabbitIn" },
  "messageType": "orders.created",
  "created": "2025-01-01T00:00:00Z",
  "message": { "...business payload..." }
}
```

`messageType` is resolved from `[MessageName("short.name")]` if present, otherwise the CLR `FullName`. `MessageTypeName.Get(type)` is the single resolver.

### INymBroker API

```csharp
broker.PostAsync<T>(endpoint, message)              // serialize + send
broker.PostAsync(endpoint, stream)                  // send pre-serialized

// Routing (all return IRouteBuilder<T> or RouteContext)
broker.Route<Order>()...Build()                     // typed route
broker.Route()...Build()                            // IAnyMessage route
broker.Route(IRouteBuilder)                         // custom builder
broker.Route(Func<RouteContext>)                    // custom factory

broker.AddFilter(IMessageFilter)
broker.AddScheduledAction(TimeSpan, Action)
broker.AddScheduledAction<T1>(TimeSpan, Action<T1>, T1)
broker.AddScheduledAction<T1,T2>(TimeSpan, Action<T1,T2>, T1, T2)
broker.AddScheduledAction<T1>(string cronExpr, Action<T1>, T1)   // Cronos cron syntax

broker.StartAsync(ct) / StopAsync(ct)              // called automatically by IHostedService
broker.ProcessAsync(raw, sourceEndpoint, ct)        // entry point for endpoint listeners
```

### Routing

`RouteContext` is an open (non-sealed) class with a `virtual Evaluate()` — subclass it for custom routing logic. Route conditions implement `IRouteCondition`:

| Condition class | Created by |
|---|---|
| `JsonRouteCondition` | `.When(Func<JsonElement, bool>)` |
| `FromRouteCondition` | `.WhenFrom(name)` |
| `NotFromRouteCondition` | `.WhenNotFrom(name)` |
| `MessageAgeRouteCondition` | `.WhenMessageIsOlderThan(TimeSpan)` |
| `AndRouteCondition` | `.And(lhs, rhs)` |
| `OrRouteCondition` | `.Or(lhs, rhs)` |

Predicates receive the **message payload** element (not the full envelope). `IAnyMessage` routes match every type.

```csharp
broker.Route<Order>()
    .To("FileOut")
    .WhenFrom("RabbitIn")
    .When(msg => msg.GetProperty("priority").GetString() == "high")
    .Build();
```

**Routing loop hazard**: any event-driven endpoint registered with the broker has `ProcessAsync` wired to its listener. Routing a message back to the same endpoint re-evaluates all routes — infinite loop without a source guard (`WhenFrom`/`WhenNotFrom`).

### Consumer Dispatch (Performance)

`ConsumerDispatcher` (separate from the broker, injected via DI) handles typed dispatch:
- **Compiled Expression lambdas** built once per message type and cached in `ConcurrentDictionary` — avoids `MethodInfo.Invoke`.
- **`IServiceScopeFactory`** creates a new async DI scope per dispatch — supports `Scoped` consumer lifetimes.
- A consumer can implement multiple `IConsume<T>` interfaces; `AddConsumer<T>()` registers all of them.

### Message Type Resolution

`MessageTypeRegistry` maps type names → CLR types using an `ImmutableDictionary`. Lookup order:

1. `[MessageName("short.name")]` attribute value
2. `Type.FullName`
3. `Type.AssemblyQualifiedName`

`SplitMessage` is pre-registered. All types used in routes or consumers are registered automatically at config time.

### Scheduled Actions

Interval-based actions fire on a timer. Cron-based actions use **Cronos** (`CronExpression.Parse`) with local timezone. Each action runs in a background `Task` managed by `ScheduledActionHandle` (implements `IAsyncDisposable`).

### Aggregator / Splitter

`SplitterImpl.Split(byte[], ISplitCondition)` partitions large payloads into `SplitMessage` parts (Base64 chunks, shared `CorrelationId`). `AggregatorImpl` collects parts by correlation ID and returns reassembled bytes when `GroupSize` is met. Incomplete aggregates expire after 2 hours.

Thread-safety: each `Aggregate` instance is lock-guarded and carries an `IsCompleted` flag. The flag prevents a second concurrent caller that obtained the same `ConcurrentDictionary` slot from reassembling or re-removing an already-completed aggregate (TOCTOU guard).

### Factory / DI

```csharp
// Core only:
services.AddNymBroker()
    .AddFileEndPoint("In",  new FileSettings { ReadPath = "in",  PostPath = "in-out" })
    .AddMemoryEndPoint("Mem")
    .AddConsumer<OrderConsumer>()
    .Build();

// With SQLite (reference NymBroker.Sqlite):
services.AddNymBroker()
    .AddSqliteEndPoint("SqlQueue", new SqliteSettings
    {
        ConnectionString = "Data Source=messages.db",
        AutoCreateTable  = true,
        LeaseTimeout     = TimeSpan.FromMinutes(5),
        MaxRetryCount    = 5
    })
    .AddConsumer<OrderConsumer>()
    .Build();

// With PostgreSQL (reference NymBroker.Postgres):
services.AddNymBroker()
    .AddPostgresEndPoint("PgQueue", new PostgresSettings
    {
        ConnectionString = "Host=localhost;Database=nymbroker;Username=postgres;Password=postgres",
        AutoCreateTable  = true
    })
    .AddConsumer<OrderConsumer>()
    .Build();

// With RabbitMQ (reference NymBroker.RabbitMq):
services.AddNymBroker()
    .AddRabbitMqEndPoint("Rabbit", new RabbitMqSettings { HostName = "localhost", ReadQueueName = "q.in" })
    .AddConsumer<OrderConsumer>()
    .Build();
```

From a config file — each transport requires its own `With*()` call:

```csharp
services.AddNymBroker()
    .LoadConfiguration("queuesettings.json")
    .WithRabbitMq()     // processes Type=RabbitMq entries
    .WithSql()          // processes Type=Sql entries (from NymBroker.Sqlite)
    .WithPostgres()     // processes Type=Postgres entries
    .AddConsumer<OrderConsumer>()
    .Build();
```

Config section key is `NymBroker` → `Endpoints[]` with `Name`, `Type` (`File|Memory|RabbitMq|Sql|Postgres`), `Config` (camelCase type-specific settings). `File` and `Memory` are processed automatically by `LoadConfiguration` without a `With*()` call.

`NymBrokerBuilder` exposes `Services` (the DI container) and `LoadedConfiguration` as public properties so extension packages in other assemblies can register their endpoint types.

### SQLite Endpoint

`SqliteEndPoint` (namespace `NymBroker.Sql`, project `NymBroker.Sqlite`) implements both `IEndPointPoll` and `IEndPointEventDriven`.

**Message lifecycle**: `Pending (0)` → `InProgress (1)` → `Completed (2)` or `Failed (3)`.

**Claiming**: a `SemaphoreSlim(1,1)` (`_dbLock`) serializes all DB operations because `SqliteConnection` is not safe for concurrent access. `ClaimMessagesAsync` runs a SELECT + per-row UPDATE inside a transaction; only rows where `rows_affected > 0` are yielded (optimistic claim). Expired leases (`LockedUntilUtc <= unixepoch()`) are reclaimable.

**Retry**: `AttemptCount` is incremented on every claim. On handler failure, messages are returned to `Pending` until `AttemptCount >= MaxRetryCount`, then marked `Failed`. `LeaseTimeout` controls how long a claimed message stays locked before another poller can reclaim it.

**Schema migration**: `EnsureSchemaAsync` detects old single-status schemas and migrates them to the full leasing schema in a transaction (backup table + INSERT SELECT).

**In-memory SQLite** (`Data Source=:memory:`) requires a single persistent connection — tests use this via `EnsureConnectionAsync` (lazy init under `_dbLock`).

### PostgreSQL Endpoint

`PostgresEndPoint` (namespace `NymBroker.Postgres`) uses the same message lifecycle as SQLite. Claiming uses `SELECT … FOR UPDATE SKIP LOCKED` so multiple application instances can poll the same table concurrently without a process-wide lock.

### Performance Design

- **RecyclableMemoryStream** (`Microsoft.IO.RecyclableMemoryStream`) for all serialization streams.
- **Compiled dispatch lambdas** in `ConsumerDispatcher` — `~10×` faster than `MethodInfo.Invoke`.
- **ImmutableList / ImmutableDictionary** for routes, filters, consumer keys — lock-free reads; writes (config-time only) use `ImmutableInterlocked.Update` for atomic CAS-loop replacement (correct for multi-key updates).
- **PropertyInfo cache** in `MessageSerializerJson.PropCache` — one reflection lookup per concrete `MessageContext<T>` type.
- **Bounded `Channel<string>`** in `MemoryQueueEndPoint` for backpressure.
- **`FileShare.ReadWrite | FileShare.Delete`** in `FileEndPoint.ReadAndArchiveAsync` — `FileShare.Delete` is required so that `File.Move` (rename) can succeed while the read handle is still open. Without it, Windows enforces sharing semantics and the rename fails with ERROR_SHARING_VIOLATION even from the same process.
- **`SemaphoreSlim(1,1)` in `SqliteEndPoint`** — SQLite single-connection; all DB ops serialized. `ReadAsync` collects rows under the lock then yields outside it to avoid holding the lock during consumer execution.

### Error Handling / Logging Guarantees

No exception is silently swallowed. The policy per layer:

| Layer | Behaviour |
|---|---|
| `NymBrokerImpl.ProcessAsync` | Deserialization failure → `LogError`, message dropped. Unresolved type with no route → `LogWarning`. |
| `ConsumerDispatcher` | No registered consumer → `LogWarning`. |
| `AggregatorImpl.PurgeExpired` | Purge count logged at `Debug`. |
| `NymBrokerImpl.StartAsync` | Any startup exception → `LogError`, scheduled actions rolled back, exception re-thrown. |
| `FileEndPoint.OnFileCreated` | Fire-and-forget handler failure → `LogError` (loop continues). |
| `FileEndPoint.ProcessExistingFilesAsync` | Per-file handler failure → `LogError` (remaining files still processed). Structural failure (e.g. directory gone) → `LogError` on outer Task.Run. |
| `FileEndPoint.ReadAndArchiveAsync` | IOException after retries → `LogWarning`, file skipped. |
| `MemoryQueueEndPoint.StartListeningAsync` | Per-message handler failure → `LogError` (loop continues). Unexpected loop termination → `LogCritical`. |
| `SqliteEndPoint` (listener loop) | Per-message handler failure → `LogError`, message returned to `Pending` or marked `Failed` after max retries. Poll error → `LogError` (loop continues). Unexpected termination → `LogCritical`. |
| `RabbitMqEndPoint` (message handler) | Handler failure → `LogError`, message nacked with `requeue: true`. |
| `RabbitMqEndPoint` (listener loop) | Unexpected loop termination → `LogCritical`. `OperationCanceledException` swallowed. |

`OperationCanceledException` is always swallowed at fire-and-forget boundaries — it represents clean shutdown, not an error.

### RabbitMQ Reliability

`RabbitMqEndPoint` uses `autoAck: false`. Every message is manually acked on success or nacked with `requeue: true` on failure. Connection and publish-channel setup use `SemaphoreSlim(1,1)` with a double-check pattern to prevent concurrent initialisation races.

### MemoryQueueEndPoint Logger

`MemoryQueueEndPoint` accepts an optional `ILogger<MemoryQueueEndPoint>? logger = null` parameter (defaults to `NullLogger`). The builder passes a proper logger via DI; tests that construct the endpoint directly with `new MemoryQueueEndPoint("name")` continue to compile without changes.

## Key Design Rules

- **Ask before architectural decisions** — built incrementally with explicit sign-off on each structural choice.
- Routes use `IRouteCondition` / `Func<JsonElement, bool>` predicates; no XML/XSLT.
- No Windows-specific endpoints (MSMQ, Event Log, etc.) — .NET Core only.
- DI: `Microsoft.Extensions.DI` only (no Autofac).
- Serialization: `System.Text.Json` only (no Newtonsoft).
- Consumers are keyed services: key = `typeof(TConsumer).Name`.
- `RouteContext` is non-sealed and `Evaluate()` is virtual — subclass for custom route logic.
- Never swallow exceptions silently — every fire-and-forget boundary and catch block must log.
- New transport endpoints must implement both `IEndPointPoll` and `IEndPointEventDriven` so the broker engine drives them automatically.
