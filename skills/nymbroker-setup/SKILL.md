---
name: nymbroker-setup
description: Add NymBroker (a .NET 10 message-processing framework) to an application — install the NuGet packages, choose transports (Memory, File, SQLite, PostgreSQL, SQL Server, RabbitMQ, Azure Service Bus), register endpoints, dead-lettering, duplicate detection, health checks and telemetry in DI. Asks the user which options they want before changing anything. Use when the user wants to set up, install, add, configure or wire up NymBroker, a message broker, a message queue or messaging in their .NET app.
---

# Set up NymBroker in an application

NymBroker posts typed messages to named **endpoints** (queues on some transport), receives them there and hands each one to a **consumer** (`IConsume<T>`), a **route** (forward to another endpoint) or a **topic** (several subscribers). Results go back to the transport, so failed messages are retried and dead-lettered.

Docs: https://github.com/nymankla/NymBroker/blob/master/docs/user-guide.md · Package: https://www.nuget.org/packages/NymBroker/

## 1. Look before asking

Inspect the project first so you only ask what the code doesn't answer:

- The target framework must be **net10.0** or later. If it's older, stop and tell the user NymBroker requires .NET 10.
- Host type: `WebApplication.CreateBuilder` (ASP.NET Core), `Host.CreateApplicationBuilder` / `Host.CreateDefaultBuilder` (worker / console), or no host.
- Whether NymBroker is already referenced (`dotnet list package`), and any existing `AddNymBroker()` call — extend it instead of adding a second one.
- Databases or brokers the app already uses (connection strings in `appsettings*.json`, packages such as Npgsql, Microsoft.Data.SqlClient, RabbitMQ.Client, Azure.Messaging.ServiceBus) — a good hint for the transport.

## 2. Ask the user (required)

**Do not install packages or edit code until the user has answered.** Ask with the question tool if you have one (one round, up to four questions, offer the options below and recommend one based on step 1); otherwise ask in plain text and wait.

1. **Transport** — where do messages live?
   - Memory — in-process only, lost on restart (tests, in-process decoupling)
   - SQLite — durable, one machine, no server needed
   - PostgreSQL / SQL Server — durable, several app instances share one queue table
   - RabbitMQ — an existing RabbitMQ broker
   - Azure Service Bus — managed queues/topics in Azure
   - File — files in a folder (file drops, simple integration)
2. **Role of this app** — produces messages, consumes them, or both? (A producer-only process uses `EndpointMode.WriteOnly` so no listener starts.)
3. **Configuration** — endpoints in code (default) or from `appsettings.json` (lets ops change connections without a rebuild)?
4. **Extras** (multi-select):
   - Dead-letter endpoint for failures (recommended for Memory and File, which cannot retry)
   - Duplicate detection (idempotent receiver): in memory, or durable in SQLite / PostgreSQL / SQL Server
   - ASP.NET Core health check
   - OpenTelemetry metrics and tracing
   - Drop messages older than a given age (TTL)

Also confirm the **endpoint names** (e.g. `Orders`) and, for a database transport, the **table name** and where the **connection string** comes from. Never invent credentials — use a placeholder in `appsettings.Development.json` or user secrets and tell the user to fill it in.

## 3. Install packages

All NymBroker packages share one version — keep them equal.

```bash
dotnet add package NymBroker                             # always: broker, Memory, File
dotnet add package Microsoft.Extensions.Hosting          # only for a console app without a host
```

| Choice | Package | Builder method (namespace) |
|---|---|---|
| SQLite | `NymBroker.Endpoint.Sqlite` | `AddSqliteEndPoint`, config `WithSql()` (`NymBroker.Endpoint.Sqlite`) |
| PostgreSQL | `NymBroker.Endpoint.Postgres` | `AddPostgresEndPoint`, `WithPostgres()` (`NymBroker.Endpoint.Postgres`) |
| SQL Server | `NymBroker.Endpoint.SqlServer` | `AddSqlServerEndPoint`, `WithSqlServer()` (`NymBroker.Endpoint.SqlServer`) |
| RabbitMQ | `NymBroker.Endpoint.RabbitMq` | `AddRabbitMqEndPoint`, `WithRabbitMq()` (`NymBroker.Endpoint.RabbitMq`) |
| Azure Service Bus | `NymBroker.Endpoint.AzureServiceBus` | `AddAzureServiceBusEndPoint`, `WithAzureServiceBus()` (`NymBroker.Endpoint.AzureServiceBus`) |
| Durable dedup, one host | `NymBroker.Idempotency.Sqlite` | `AddSqliteIdempotency` (`NymBroker.Idempotency.Sqlite`) |
| Durable dedup, shared | `NymBroker.Idempotency.Postgres` / `.SqlServer` | `AddPostgresIdempotency` / `AddSqlServerIdempotency` |

## 4. Register the broker

Core namespaces: `NymBroker.Core.DI` (`AddNymBroker`), `NymBroker.Core.Endpoint` (`EndpointMode`), `NymBroker.Core.Endpoint.File` (`FileSettings`), `NymBroker.Core.Impl` (`INymBroker`).

`AddNymBroker()` returns a builder; finish with **`.Build()`**, which registers `INymBroker` as a singleton plus a hosted service that starts and stops the broker with the host.

```csharp
builder.Services.AddNymBroker()
    .AddSqliteEndPoint("Orders", new SqliteSettings
    {
        ConnectionString = builder.Configuration.GetConnectionString("Queue")
            ?? throw new InvalidOperationException("Connection string 'Queue' is missing."),
        TableName        = "orders_queue",
        AutoCreateTable  = true
    })
    .AddConsumer<OrderCreatedConsumer>()      // add consumers with the nymbroker-consumer skill
    .Build();
```

Settings per transport (only set what differs from the defaults):

| Transport | Settings | Typical values |
|---|---|---|
| Memory | `AddMemoryEndPoint(name, capacity: 1000)` | — |
| File | `FileSettings { ReadPath, PostPath }` | folders to watch / write to |
| SQLite | `SqliteSettings { ConnectionString, TableName, AutoCreateTable, BatchSize, PollInterval, LeaseTimeout, MaxRetryCount }` | `Data Source=queue.db` |
| PostgreSQL | `PostgresSettings { ConnectionString, TableName, AutoCreateTable, UseNotifications, … }` | same SQL options as SQLite |
| SQL Server | `SqlServerSettings { ConnectionString, TableName = "dbo.x", AutoCreateTable, … }` | same SQL options |
| RabbitMQ | `RabbitMqSettings { HostName, ReadQueueName, WriteQueueName }` | omit `ReadQueueName` for a producer |
| Service Bus | `AzureServiceBusSettings { ConnectionString or FullyQualifiedNamespace + Credential, QueueName or TopicName (+ SubscriptionName), MaxConcurrentCalls, PrefetchCount }` | `Credential` (e.g. `DefaultAzureCredential`) needs code, not JSON |

**Role:** producer-only → pass `EndpointMode.WriteOnly` as the last argument (`AddSqliteEndPoint("Orders", settings, EndpointMode.WriteOnly)`). Consumer or both → default `ReadWrite`.

### From appsettings.json instead

```json
{
  "NymBroker": {
    "Endpoints": [
      { "Name": "Orders", "Type": "Sql", "Config": { "connectionString": "Data Source=queue.db", "tableName": "orders_queue", "autoCreateTable": true } }
    ]
  }
}
```

```csharp
using NymBroker.Core.Factory.Configuration;

builder.Services.AddNymBroker()
    .ApplyConfiguration(BrokerConfigurationReader.Read(builder.Configuration))   // section "NymBroker"
    .WithSql()                       // one With…() per add-on transport; Memory and File need none
    .AddConsumer<OrderCreatedConsumer>()
    .Build();
```

`Type` values: `Memory`, `File`, `Sql` (SQLite), `Postgres`, `SqlServer`, `RabbitMq`, `AzureServiceBus`. An entry whose `With…()` is missing is silently ignored and posting to it fails with "No endpoint registered". `"Mode": "WriteOnly"` sets the mode. Consumers, routes and topics always stay in code.

## 5. Extras

```csharp
// Dead-letter endpoint — always WriteOnly, or failures loop back into processing
.AddFileEndPoint("DeadLetters", new FileSettings { PostPath = "dead-letters" }, EndpointMode.WriteOnly)
.WithDeadLetterEndpoint("DeadLetters")

// Duplicate detection — in memory, per process
.AddIdempotentReceiver(TimeSpan.FromHours(24))
// … or durable (pick one store per broker)
.AddSqlServerIdempotency(new SqlServerIdempotencySettings { ConnectionString = cs, TableName = "dbo.nymbroker_idempotency" })
.AddPostgresIdempotency(new PostgresIdempotencySettings { ConnectionString = cs, TableName = "nymbroker_idempotency" })
.AddSqliteIdempotency(new SqliteIdempotencySettings { ConnectionString = "Data Source=idempotency.db" })

// TTL
.DiscardMessagesOlderThan(TimeSpan.FromMinutes(15))
```

- Dead-letter endpoint: transports with their own dead-letter handling (SQL tables, RabbitMQ, Service Bus) retry and dead-letter natively; the dead-letter endpoint catches Memory/File failures. Without it those failures are only logged.
- Health check (ASP.NET Core): `builder.Services.AddHealthChecks().AddNymBroker(name: "nymbroker", tags: ["ready"]);` then `app.MapHealthChecks("/health");`. Mark optional endpoints with `.ConfigureHealthCheck(o => o.NonCritical("Audit"))`.
- OpenTelemetry: `.WithMetrics(m => m.AddMeter("NymBroker", "NymBroker.Resilience"))` and `.WithTracing(t => t.AddSource("NymBroker"))`.

## 6. Sending

Inject `INymBroker` and post to an endpoint by name:

```csharp
await broker.PostAsync("Orders", new OrderCreated("ORD-1", 49.90m), ct);
await broker.PostBatchAsync("Orders", orders, ct);   // many messages, one round trip where supported
```

In a console app without DI-driven startup, call `await host.StartAsync()` before relying on consumers; posting before start is fine — messages wait in the transport.

## 7. Finish

1. Create at least one message type and consumer with the **nymbroker-message** and **nymbroker-consumer** skills (or tell the user to).
2. `dotnet build` and fix errors.
3. Summarize for the user: packages added, endpoints and their modes, where the connection string must be set, and anything left to configure (for example the RabbitMQ dead-letter exchange or the Service Bus queue, which NymBroker does not create).

## Pitfalls

- **Routing loops**: routing or topic-forwarding a message to an endpoint this broker also listens on re-processes it. Guard with `.WhenFrom("Source")` / `.WhenNotFrom("Dest")` or make the destination `WriteOnly`.
- **One consumer per message type.** Several handlers → a topic with `ISubscribe<T>` subscribers.
- Memory and File cannot redeliver: a crash mid-message loses it. Use a database or broker transport when that matters.
- SQLite is for one machine; use PostgreSQL or SQL Server for several instances.
- Call `AddNymBroker()` once per service collection.
