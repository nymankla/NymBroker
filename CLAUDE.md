# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A .NET 10 enterprise message processing framework following EIP (Enterprise Integration Patterns). Messages flow as JSON envelopes through a configurable pipeline:

```
Source Endpoint → Deserialize → Filter → Router → Consumer / Destination Endpoint
                                              ↕
                                    Aggregator / Splitter
```

Endpoints: **RabbitMQ**, **Azure Service Bus**, **SQLite**, **PostgreSQL**, **SQL Server**, **File**, **Memory** (in-process / tests).

## Commands

Run from the solution root (`e:\utv\POC\NymBroker`):

```bash
dotnet build
dotnet test
dotnet test --project NymBroker.Tests -- --filter-class "*SerializerTests"   # single test class (MTP runner, see global.json)
dotnet run --project samples/NymBroker.Sample            # fluent API demo
dotnet run --project samples/NymBroker.ConfigSample      # JSON config demo
dotnet run --project samples/NymBroker.SqlSample         # SQLite endpoint demo
dotnet run --project samples/NymBroker.SqlServerSample   # SQL Server endpoint demo (run setup-sqlserver.ps1 first)
dotnet run --project samples/NymBroker.AzureServiceBusSample   # Service Bus demo incl. dead-letter queue (run setup-servicebus.ps1 first)
dotnet run --project samples/NymBroker.Benchmarks        # throughput benchmark

# PostgreSQL / SQL Server integration tests (skipped unless the env vars are set)
$env:NYMBROKER_POSTGRES_CS  = "Host=localhost;Database=nymbroker;Username=postgres;Password=postgres"
$env:NYMBROKER_SQLSERVER_CS = "Server=localhost,1433;Database=nymbroker;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True"
$env:NYMBROKER_SERVICEBUS_CS = "Endpoint=sb://localhost:5673;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
dotnet test --project NymBroker.Tests -- --filter-class "*EndPointTests"

# Local infrastructure (Docker Desktop required) — services defined in scripts/docker-compose.yml
./scripts/setup-rabbitmq.ps1          # start + wait for healthy
./scripts/setup-rabbitmq.ps1 -Stop    # stop
./scripts/setup-rabbitmq.ps1 -Logs    # tail logs
./scripts/setup-postgres.ps1          # PostgreSQL on localhost:5432, db nymbroker (postgres/postgres); same -Stop/-Logs
./scripts/setup-sqlserver.ps1         # SQL Server 2022 on localhost,1433, creates db nymbroker (sa / NymBroker!Dev123); same -Stop/-Logs
./scripts/setup-servicebus.ps1        # Azure Service Bus emulator, AMQP on localhost:5673 (5672 is RabbitMQ); entities in scripts/servicebus/Config.json; same -Stop/-Logs
```

Note: `setup-rabbitmq.ps1 -Stop` and `setup-postgres.ps1 -Stop` run `docker compose down`, which stops **all** services in the compose file. `setup-sqlserver.ps1 -Stop` and `setup-servicebus.ps1 -Stop` stop only their own service. The Service Bus emulator stores its state in the `sqlserver` service (it starts with it).

## Architecture

### Solution Layout

| Project | Role |
|---|---|
| `NymBroker.Core` | Framework core — endpoints, serializer, routing, broker engine, factory. No transport dependency. |
| `NymBroker.RabbitMq` | Optional add-on — `RabbitMqEndPoint`, `RabbitMqSettings`, `AddRabbitMqEndPoint`/`WithRabbitMq`. |
| `NymBroker.Resilience` | Dependency-free retry policy (`RetryPolicy`, `RetryOptions`) — constant/exponential backoff with optional jitter. Referenced by Core; used by `FileEndPoint` (IOException retry) and `RabbitMqEndPoint` (reconnect). Replaces Polly. Options documented in [NymBroker.Resilience/README.md](NymBroker.Resilience/README.md). |
| `NymBroker.Sqlite` | Optional add-on — `SqliteEndPoint`, `SqliteSettings`, `AddSqliteEndPoint`/`WithSql`. Uses Dapper + `Microsoft.Data.Sqlite`. |
| `NymBroker.Postgres` | Optional add-on — `PostgresEndPoint`, `PostgresSettings`, `AddPostgresEndPoint`/`WithPostgres`. Uses Npgsql. |
| `NymBroker.SqlServer` | Optional add-on — `SqlServerEndPoint`, `SqlServerSettings`, `AddSqlServerEndPoint`/`WithSqlServer`, config type `SqlServerEndPointType.SqlServer`. Uses `Microsoft.Data.SqlClient`. |
| `NymBroker.AzureServiceBus` | Optional add-on — `AzureServiceBusEndPoint`, `AzureServiceBusSettings`, `AddAzureServiceBusEndPoint`/`WithAzureServiceBus`, config type `AzureServiceBusEndPointType.AzureServiceBus`. Uses `Azure.Messaging.ServiceBus` (no `Azure.Identity` dependency). |
| `NymBroker.Tests` | xUnit tests — uses Memory and SQLite `:memory:` endpoints; no RabbitMQ/Postgres/file I/O. PostgreSQL / SQL Server / Service Bus integration tests (`PostgresEndPointTests`, `SqlServerEndPointTests`, `AzureServiceBusEndPointTests`) run only when `NYMBROKER_POSTGRES_CS` / `NYMBROKER_SQLSERVER_CS` / `NYMBROKER_SERVICEBUS_CS` are set; otherwise they are skipped (their unit and health-check tests always run). |
| `samples/NymBroker.Sample` | Runnable demo with file + memory endpoints, scheduled actions, routing |
| `samples/NymBroker.ConfigSample` | Demo using `queuesettings.json` for endpoint configuration |
| `samples/NymBroker.SqlSample` | SQLite endpoint demo — posts orders, broker claims and dispatches |
| `samples/NymBroker.SqlServerSample` | SQL Server endpoint demo — same flow as the Postgres sample; needs `scripts/setup-sqlserver.ps1` |
| `samples/NymBroker.AzureServiceBusSample` | Service Bus demo — a failing order is retried, dead-lettered by Service Bus, then read back via `ReadDeadLetterQueue`; needs `scripts/setup-servicebus.ps1` |
| `samples/NymBroker.Benchmarks` | Throughput + allocation benchmark — Memory, File, SQLite and split scenarios always; Postgres, SQL Server, Azure Service Bus (emulator queue `nymbroker.bench`) and RabbitMQ scenarios when reachable on localhost (skipped otherwise) |

### Key Abstractions

```
IEndPoint                 ← all transports: Mode, PostAsync(byte[]), HealthCheck()
  IEndPointEventDriven    ← inbound: StartListeningAsync(handler) / StopListeningAsync; UsesNativeDeadLetter
EndpointMode              ← ReadWrite (default) | ReadOnly | WriteOnly
ProcessResult             ← what the handler returns: Completed | Retry | DeadLetter(Reason, Description); FailureText
DeadLetterReasons         ← DeserializationFailed, Expired, UnknownCompression, ConsumerFailed, TopicDeliveryFailed

IMessageContext<T>        ← typed envelope (Id, CorrelationId, Address, MessageType, Created)
RawMessageContext          ← internal deserialized form; holds JsonElement RawMessage for deferred typing

INymBroker            ← engine facade (see below)
IConsume<T>               ← consumer contract; implement + register via AddConsumer<T>()
IRouteBuilder<T>          ← fluent route definition (see Routing section)
IRouteCondition           ← composable predicate evaluated on (IMessageContext, JsonElement)
```

`NymBrokerImpl.StartAsync` calls `StartListeningAsync` on every `IEndPointEventDriven` endpoint whose `Mode` is not `WriteOnly`, wiring the handler to `ProcessAsync(raw, endpointName, ct)`, which returns a `ProcessResult` the endpoint settles the message by (ack / redeliver / dead-letter). A plain `IEndPoint` is a send-only sink. Pull-based transports (`SqliteEndPoint`, `PostgresEndPoint`) still implement `IEndPointEventDriven` — their poll loop runs on `Task.Run` and calls the handler for each claimed message. `StartAsync` throws if a route, topic, dead-letter or wire-tap target is `ReadOnly`.

**Processing results and native dead-lettering (0.2.0, #39).** `ProcessAsync` never throws (except `OperationCanceledException`); it returns:

| Situation | Source has `UsesNativeDeadLetter = true` | Otherwise (Memory, File, opt-out, `PublishAsync`) |
|---|---|---|
| Success, filtered, no consumer | `Completed` | `Completed` |
| Undecodable / expired / unknown compression | `DeadLetter(reason)` | post to the `WithDeadLetterEndpoint` endpoint, `Completed` |
| Consumer or topic fan-out throws | `Retry(ex)` — the transport redelivers, dead-letters at its limit | post to the dead-letter endpoint, `Completed` |
| A route's destination throws (or any unexpected exception) | `Retry(ex)` | `Retry(ex)` |

`UsesNativeDeadLetter` is driven by `UseNativeDeadLetter` (default `true`) in `RabbitMqSettings`, `AzureServiceBusSettings`, `SqliteSettings`, `PostgresSettings`, `SqlServerSettings`; Memory/File return `false`. For split messages the part that completes the group carries the reassembled message's result. `PublishAsync<T>` rethrows a `Retry`'s exception (no transport to redeliver). The processing-duration histogram carries a `result` tag (`completed`/`retry`/`dead_letter`). Helper: `UsesNativeDeadLetter(source)` / `DeadLetterAsync(...)` in `NymBrokerImpl.Processing.cs`.

`MemoryQueueEndPoint`, `FileEndPoint` and `SqliteEndPoint` also expose a non-interface `ReadAsync` (drains currently available items) — used by tests and samples to inspect what was posted. There is no `IEndPointPoll` interface any more.

See [docs/writing-an-endpoint.md](docs/writing-an-endpoint.md) for how to implement a new endpoint.

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
// Both overloads take a trailing `int? splitThresholdBytes = null` and `bool compress = true`:
// when a threshold is set and the serialized envelope exceeds it, the message is transparently
// split into SplitMessage parts (via ISplitter) and posted individually; ProcessAsync reassembles
// them on arrival. When compress is also true, the envelope is compressed (ICompressor, Brotli by
// default) before splitting whenever that actually shrinks it — offsets Base64's ~33% overhead for
// compressible (text/JSON) payloads. See "Aggregator / Splitter" below.

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
broker.ProcessAsync(raw, sourceEndpoint, ct)        // entry point for endpoint listeners → Task<ProcessResult>
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

`PostAsync<T>`/`PostAsync(Stream)` can drive this automatically: pass `splitThresholdBytes` and `NymBrokerImpl` calls `ISplitter` internally (via `DefaultSplitCondition(splitThresholdBytes)`) when the serialized envelope exceeds it, posting each part to the same endpoint instead of manually calling `ISplitter.Split` yourself.

**Compression**: since Base64 inflates each chunk ~33%, `PostAsync` also compresses the envelope via `ICompressor` (`BrotliCompressor` by default, built on `System.IO.Compression.BrotliStream`) before splitting, whenever `compress` is true (the default) *and* compression actually reduces the size — this typically more than cancels out the Base64 overhead for compressible text/JSON payloads, and the size check means already-compressed/binary payloads fall back to uncompressed splitting automatically rather than paying compression cost for no benefit. Compressed parts carry `SplitMessage.Compression` (the `ICompressor.Name`, e.g. `"brotli"`); `ProcessAsync`'s aggregator branch decompresses using the same `ICompressor` before treating the reassembled bytes as envelope JSON, and drops the message with a logged error if the codec name doesn't match. If compression alone brings the payload under `splitThresholdBytes`, it still ships as a single-part `SplitMessage` group (`GroupSize = 1`) — compressed bytes aren't valid JSON on their own, so they still need the `SplitMessage` carrier even when they'd otherwise fit.

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

// With SQL Server (reference NymBroker.SqlServer):
services.AddNymBroker()
    .AddSqlServerEndPoint("SqlServerQueue", new SqlServerSettings
    {
        ConnectionString = "Server=localhost,1433;Database=nymbroker;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True",
        TableName        = "dbo.orders",
        AutoCreateTable  = true
    })
    .AddConsumer<OrderConsumer>()
    .Build();

// With Azure Service Bus (reference NymBroker.AzureServiceBus):
services.AddNymBroker()
    .AddAzureServiceBusEndPoint("Orders", new AzureServiceBusSettings
    {
        ConnectionString = "<namespace or emulator connection string>",   // or FullyQualifiedNamespace + Credential
        QueueName        = "orders"                                       // or TopicName (+ SubscriptionName to receive)
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
    .WithSqlServer()    // processes Type=SqlServer entries (from NymBroker.SqlServer)
    .WithAzureServiceBus()  // processes Type=AzureServiceBus entries (from NymBroker.AzureServiceBus)
    .AddConsumer<OrderConsumer>()
    .Build();
```

Config section key is `NymBroker` → `Endpoints[]` with `Name`, `Type`, `Config` (camelCase type-specific settings). `Type` is an **open string**, not an enum: `EndPointType` is a static class of string constants for the built-in types (`File|Memory|RabbitMq|Sql|Postgres`, also listed in `EndPointType.BuiltIn`). Unknown types load without error and are left for their package's `With*()` extension, which matches with `ep.IsType(name)` (case-insensitive). New transports define their type-name constant in their own package; Core does not change (e.g. `SqlServerEndPointType.SqlServer = "SqlServer"` lives in `NymBroker.SqlServer`). `File` and `Memory` are processed automatically by `LoadConfiguration` without a `With*()` call.

`NymBrokerBuilder` exposes `Services` (the DI container) and `LoadedConfiguration` as public properties so extension packages in other assemblies can register their endpoint types.

### SQLite Endpoint

`SqliteEndPoint` (namespace `NymBroker.Sql`, project `NymBroker.Sqlite`) implements `IEndPointEventDriven` (internal poll loop) plus a non-interface `ReadAsync`.

**Message lifecycle**: `Pending (0)` → `InProgress (1)` → `Completed (2)` or `Failed (3)`.

**Claiming**: a `SemaphoreSlim(1,1)` (`_dbLock`) serializes all DB operations because `SqliteConnection` is not safe for concurrent access. `ClaimMessagesAsync` runs a SELECT + per-row UPDATE inside a transaction; only rows where `rows_affected > 0` are yielded (optimistic claim). Expired leases (`LockedUntilUtc <= unixepoch()`) are reclaimable.

**Retry**: `AttemptCount` is incremented on every claim. On `Retry` (or a handler exception), messages are returned to `Pending` until `AttemptCount >= MaxRetryCount`, then marked `Failed`; `DeadLetter` marks them `Failed` at once with `"{Reason}: {Description}"` in `LastError` (same in the Postgres / SQL Server endpoints). `StopListeningAsync` awaits the loop, and a handled message's outcome is written even during shutdown. `LeaseTimeout` controls how long a claimed message stays locked before another poller can reclaim it.

**Schema migration**: `EnsureSchemaAsync` detects old single-status schemas and migrates them to the full leasing schema in a transaction (backup table + INSERT SELECT).

**In-memory SQLite** (`Data Source=:memory:`) requires a single persistent connection — tests use this via `EnsureConnectionAsync` (lazy init under `_dbLock`).

### PostgreSQL Endpoint

`PostgresEndPoint` (namespace `NymBroker.Postgres`) uses the same message lifecycle as SQLite. Claiming uses `SELECT … FOR UPDATE SKIP LOCKED` so multiple application instances can poll the same table concurrently without a process-wide lock. Measured on the `setup-postgres.ps1` container, the 0.1.5 rework took backlog drain with default settings from ~80 to ~3 000–5 000 msg/s, with `BatchSize=50` from ~4 000 to ~9 000 msg/s, and with 300k completed rows from ~1 600 to ~10 500 msg/s.

- **Polling**: back to back while batches come back non-empty. Only after an empty poll does it wait for `NOTIFY` / `PollInterval`. (≤ 0.1.4 waited after *every* batch, capping default settings at `BatchSize / PollInterval` ≈ 100 msg/s.)
- **One round trip and one commit per batch**: `FinalizeAndClaimAsync` sends the finalize of batch N and the claim of batch N+1 as one `NpgsqlBatch`, which PostgreSQL executes as one implicit transaction.
- **Finalize** (`PostgresQueueSql.FinalizeMessages`) writes mixed outcomes in one `UPDATE … FROM unnest(@queueIds, @attempts, @statuses, @errors)`, guarded by `status = InProgress AND attempt_count = source.attempt` so a poller whose lease expired cannot overwrite a re-claimed row.
- **Schema**: one partial index `(created_at_utc, queue_id) WHERE status IN (0, 1)`. Claim predicates inline the status **literals** — the planner can only use a partial index when it can prove the predicate at plan time (not with parameters; Npgsql auto-prepares). `CreateSchema` drops the two full status indexes created by ≤ 0.1.4 (`ix_<table>_status_created`, `ix_<table>_status_locked_until`) on existing tables.
- **Shutdown**: `StopListeningAsync` awaits the loop, which writes the results of already-handled messages with a fresh 10 s token (`FinalizeOnShutdownAsync`).
- **LISTEN failures** (startup or later) fall back to timer polling with a `LogWarning` and retry on the next idle cycle; ≤ 0.1.4 let an unreachable database at startup kill the listener loop.
- **Inserts** are one autocommit `INSERT` (+ `NOTIFY`) per `PostAsync`, bound by the WAL flush (~1.7 ms on Docker Desktop). Removing `NOTIFY` was measured and gained little.

### SQL Server Endpoint

`SqlServerEndPoint` (namespace `NymBroker.SqlServer`) has the same lifecycle, leases, retry/`Failed` handling, logging and loop structure (back-to-back drain, finalize + claim in one round trip, shutdown finalize) as `PostgresEndPoint`, but the SQL is written for SQL Server. Each choice below was measured against the straight port (`scripts/setup-sqlserver.ps1` container): backlog drain went from ~3 000 to ~4 500–6 000 msg/s, and from ~2 500 to ~5 000 msg/s with 300k completed rows in the table.

- **One round trip and one commit per batch** (`SqlServerQueueSql.FinalizeAndClaim`): the results of batch N are written in the same T-SQL batch as the claim of batch N+1. The transaction is inside the SQL text (`SET XACT_ABORT ON; BEGIN TRANSACTION … COMMIT`), not `SqlConnection.BeginTransaction`, which would cost two extra round trips.
- **Claiming** uses an updatable CTE with `WITH (UPDLOCK, READPAST, ROWLOCK)` (the SQL Server equivalent of `SKIP LOCKED`) and `OUTPUT inserted.*`, ordered by `queue_id`.
- **Finalize** reads `{id, attempt, status, error}` items from one JSON `NVARCHAR(MAX)` parameter with `OPENJSON` (SQL Server 2016+). It must drive the join (`FROM OPENJSON(...) INNER LOOP JOIN <table> WITH (FORCESEEK)`): without the hints the optimizer cannot estimate `OPENJSON`'s row count and scans the whole table (~8× more CPU). The guard `status = InProgress AND attempt_count = source.attempt` stops a poller whose lease expired from overwriting a re-claimed row.
- **Schema**: clustered PK on the IDENTITY `queue_id`; a single **filtered index** `(queue_id) INCLUDE (status, locked_until_utc) WHERE status IN (0, 1)`, so claiming stays cheap as Completed/Failed rows accumulate. `message_id` has no unique index (it is a per-insert GUID nobody looks up, and a random-GUID index costs a page-scattered write per insert). Status values are **inlined literals**, not parameters — SQL Server only uses a filtered index when it can match the predicate at compile time.
- **Polling**: back to back while batches come back non-empty; `PollInterval` only applies after an empty poll. No `LISTEN/NOTIFY` equivalent.
- **Shutdown**: `StopListeningAsync` awaits the loop, which then writes the results of already-handled messages with a fresh 10 s token (`FinalizeOnShutdownAsync`). Claimed-but-unhandled messages wait for lease expiry.
- **Inserts** are one autocommit `INSERT` per `PostAsync` (durable on return), so a single producer is bound by the transaction-log flush (~3 ms on Docker Desktop). Concurrent producers benefit from SQL Server's own group commit. Don't use `DELAYED_DURABILITY` for the queue — it can lose committed messages.
- **Connections**: one pooled `SqlConnection` per operation (`SqlConnection` is not thread-safe).

### Azure Service Bus Endpoint

`AzureServiceBusEndPoint` (namespace `NymBroker.AzureServiceBus`) is push-based: a `ServiceBusProcessor` (peek-lock, `AutoCompleteMessages = false`, `MaxConcurrentCalls` default 1) calls the broker and settles by the `ProcessResult` in `ServiceBusSettlement` (internal, unit-tested through `IServiceBusMessageSettler`):

- `Completed` → `CompleteMessageAsync`; `Retry` (or a handler exception) → `AbandonMessageAsync` — Service Bus redelivers and dead-letters with `MaxDeliveryCountExceeded` at the entity's `MaxDeliveryCount`; `DeadLetter` → `DeadLetterMessageAsync(reason, description)` (truncated to 4 KB).
- `ReadDeadLetterQueue = true` receives from the DLQ sub-queue; there a `DeadLetter` result **completes** the message (it is already dead-lettered) with a warning.
- One lazily created `ServiceBusClient` + `ServiceBusSender` per endpoint (thread-safe, long-lived). Transient faults use the SDK's own retry options — no `RetryPolicy` wrapper. `ProcessErrorAsync` logs every processor error.
- Auth: `ConnectionString`, or `FullyQualifiedNamespace` + `Credential` (`TokenCredential`, code-only, `[JsonIgnore]`). `AzureServiceBusSettings.Validate` runs at registration and in the constructor.
- `HealthCheck()` peeks the entity (or opens the sender for a send-only topic endpoint) with a 5 s timeout; unhealthy if the processor stopped unexpectedly.
- `StopListeningAsync` → `StopProcessingAsync` (waits for in-flight handlers); a handler cancelled during shutdown leaves its message unsettled (redelivered after the lock expires).
- Emulator quirks seen in tests: `ReceiveAndDelete` reads from a DLQ returned nothing (peek-lock works); `DeadLetterSource` is empty. Integration tests share the fixed `nymbroker.tests` queue (MaxDeliveryCount 3) and drain it before each test.

### Performance Design

- **RecyclableMemoryStream** (`Microsoft.IO.RecyclableMemoryStream`) for all serialization streams.
- **Compiled dispatch lambdas** in `ConsumerDispatcher` — `~10×` faster than `MethodInfo.Invoke`.
- **ImmutableList / ImmutableDictionary** for routes, filters, consumer keys — lock-free reads; writes (config-time only) use `ImmutableInterlocked.Update` for atomic CAS-loop replacement (correct for multi-key updates).
- **PropertyInfo cache** in `MessageSerializerJson.PropCache` — one reflection lookup per concrete `MessageContext<T>` type.
- **Bounded `Channel<byte[]>`** in `MemoryQueueEndPoint` for backpressure.
- **`FileShare.ReadWrite | FileShare.Delete`** in `FileEndPoint.ReadAndArchiveAsync` — `FileShare.Delete` is required so that `File.Move` (rename) can succeed while the read handle is still open. Without it, Windows enforces sharing semantics and the rename fails with ERROR_SHARING_VIOLATION even from the same process.
- **`SemaphoreSlim(1,1)` in `SqliteEndPoint`** — SQLite single-connection; all DB ops serialized. `ReadAsync` collects rows under the lock then yields outside it to avoid holding the lock during consumer execution.

### Error Handling / Logging Guarantees

No exception is silently swallowed. The policy per layer:

| Layer | Behaviour |
|---|---|
| `NymBrokerImpl.ProcessAsync` | Deserialization failure → `LogError`, then dead-lettered (native `DeadLetter` or the broker's dead-letter endpoint). Unresolved type with no route → `LogWarning`. Every dead-lettering → `LogWarning` with its reason. Unexpected exception → `LogError`, returns `Retry`. |
| `ConsumerDispatcher` | No registered consumer → `LogWarning`. |
| `AggregatorImpl.PurgeExpired` | Purge count logged at `Debug`. |
| `NymBrokerImpl.StartAsync` | Any startup exception → `LogError`, scheduled actions rolled back, exception re-thrown. |
| `FileEndPoint.OnFileCreated` | Fire-and-forget handler failure, or a non-`Completed` result → `LogError` (the file is already renamed, no retry; loop continues). |
| `FileEndPoint.ProcessExistingFilesAsync` | Per-file handler failure → `LogError` (remaining files still processed). Structural failure (e.g. directory gone) → `LogError` on outer Task.Run. |
| `FileEndPoint.ReadAndArchiveAsync` | IOException after retries → `LogWarning`, file skipped. |
| `MemoryQueueEndPoint.StartListeningAsync` | Per-message handler failure, or a non-`Completed` result → `LogError` (no redelivery; loop continues). Unexpected loop termination → `LogCritical`. |
| `SqliteEndPoint` (listener loop) | Per-message handler exception → `LogError`; `Retry` → returned to `Pending` or marked `Failed` after max retries; `DeadLetter` → `Failed` at once + `LogWarning`. Poll error → `LogError` (loop continues). Unexpected termination → `LogCritical`. |
| `PostgresEndPoint` / `SqlServerEndPoint` (listener loop) | Same as `SqliteEndPoint`, plus `LogWarning` when a message reaches the terminal `Failed` state. Health check failure → `LogError`, `Unhealthy` returned (never throws). A failed finalize-and-claim is retried on the next cycle (transaction rolled back); if writing results on shutdown fails → `LogWarning` (messages redelivered after lease expiry). `PostgresEndPoint`: a failed LISTEN / notification wait → `LogWarning`, falls back to timer polling. |
| `AzureServiceBusEndPoint` | Handler exception → `LogError`, treated as `Retry`. `Retry` / `DeadLetter` → `LogWarning`. Settle failure (e.g. lost lock) → `LogError` (redelivered after the lock expires). Processor errors (`ProcessErrorAsync`) → `LogError`. Health check failure → `LogError`, `Unhealthy`. |
| `RabbitMqEndPoint` (message handler) | Handler exception → `LogError`, treated as `Retry`. `Retry` → `LogWarning`, nacked with `requeue: true` (poison after redelivery: `requeue: false`). `DeadLetter` → `LogWarning`, nacked with `requeue: false` (the queue's DLX). |
| `RabbitMqEndPoint` (listener loop) | Unexpected loop termination → `LogCritical`. `OperationCanceledException` swallowed. |

`OperationCanceledException` is always swallowed at fire-and-forget boundaries — it represents clean shutdown, not an error.

### RabbitMQ Reliability

`RabbitMqEndPoint` uses `autoAck: false`. Every message is settled by its `ProcessResult`: `Completed` → ack (batched by `BatchAckSize`); `Retry` → nack `requeue: true`, or `requeue: false` once it was already redelivered (`RejectRedeliveredFailures`); `DeadLetter` → nack `requeue: false`, so it reaches the queue's dead-letter exchange if one is configured (RabbitMQ records `x-death` reason `rejected`; our reason is only logged). Connection and publish-channel setup use `SemaphoreSlim(1,1)` with a double-check pattern to prevent concurrent initialisation races.

### Retry Policy (NymBroker.Resilience)

Transient-failure retries use `RetryPolicy` from the dependency-free `NymBroker.Resilience` project (it replaced Polly — do not re-add Polly). Build one `RetryPolicy(new RetryOptions { ... })` per endpoint and reuse it; call `ExecuteAsync(token => ..., ct)`. Default `ShouldHandle` retries everything except `OperationCanceledException`; `OnRetry` should log (no silent retries). All options, defaults, backoff/jitter formulas and execution rules are documented in [NymBroker.Resilience/README.md](NymBroker.Resilience/README.md).

### MemoryQueueEndPoint Logger

`MemoryQueueEndPoint` accepts an optional `ILogger<MemoryQueueEndPoint>? logger = null` parameter (defaults to `NullLogger`). The builder passes a proper logger via DI; tests that construct the endpoint directly with `new MemoryQueueEndPoint("name")` continue to compile without changes.

## Key Design Rules

- **Ask before architectural decisions** — built incrementally with explicit sign-off on each structural choice.
- **Bump the patch version on every push to `master`** (the main branch). Run `.\scripts\pack.ps1 -BumpPatch` — it updates `<Version>` in `Directory.Build.props` (e.g. `0.1.0` → `0.1.1`) and packs — then commit the bump together with the change before pushing. Minor/major bumps (`-BumpMinor` / `-BumpMajor`) are the user's call.
- Routes use `IRouteCondition` / `Func<JsonElement, bool>` predicates; no XML/XSLT.
- No Windows-specific endpoints (MSMQ, Event Log, etc.) — .NET Core only.
- DI: `Microsoft.Extensions.DI` only (no Autofac).
- Serialization: `System.Text.Json` only (no Newtonsoft).
- Consumers are keyed services: key = `typeof(TConsumer).Name`.
- `RouteContext` is non-sealed and `Evaluate()` is virtual — subclass for custom route logic.
- Never swallow exceptions silently — every fire-and-forget boundary and catch block must log.
- New transport endpoints that receive messages must implement `IEndPointEventDriven` (pull-based transports run their own poll loop inside it) so the broker engine drives them automatically, and settle each message by the handler's `ProcessResult`. Return `UsesNativeDeadLetter => settings.UseNativeDeadLetter` only if the transport has its own dead-letter queue. Send-only sinks implement plain `IEndPoint` with `Mode => EndpointMode.WriteOnly`.
