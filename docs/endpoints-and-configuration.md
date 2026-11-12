# Endpoints and configuration

[← User guide](user-guide.md)

An **endpoint** is a named connection to a transport. You post to endpoints by name, and the broker listens on every endpoint that can receive. One broker can use any mix of endpoints.

## Choosing an endpoint

| Endpoint | Package | Durable | Receives by | Failed messages | Batch send | Best for |
|---|---|---|---|---|---|---|
| **Memory** | `NymBroker` | no (process memory) | in-process channel | broker dead-letter endpoint | one by one | tests, in-process decoupling |
| **File** | `NymBroker` | yes (files) | file watcher + periodic scan | broker dead-letter endpoint | one by one | file drops, simple integration |
| **SQLite** | `NymBroker.Endpoint.Sqlite` | yes | polling a table | retried, then `Failed` row | one transaction | single-process apps, edge devices |
| **PostgreSQL** | `NymBroker.Endpoint.Postgres` | yes | polling + `LISTEN/NOTIFY` | retried, then `Failed` row | one statement | multi-instance apps already on Postgres |
| **SQL Server** | `NymBroker.Endpoint.SqlServer` | yes | polling a table | retried, then `Failed` row | one statement | multi-instance apps already on SQL Server |
| **MySQL** | `NymBroker.Endpoint.MySql` | yes | polling a table | retried, then `Failed` row | one statement | multi-instance apps already on MySQL 8.0+ / MariaDB 10.6+ |
| **RabbitMQ** | `NymBroker.Endpoint.RabbitMq` | yes | push (consumer) | requeued, then the queue's dead-letter exchange | published together, then all confirms awaited (not atomic) | a message broker you already run |
| **Azure Service Bus** | `NymBroker.Endpoint.AzureServiceBus` | yes | push (processor) | abandoned, then the queue's dead-letter queue | Service Bus batches | Azure, managed queues and topics |
| **HTTP** | `NymBroker.Endpoint.Http` | — (send only) | — (receive in your web app, see below) | the source retries; see [HTTP notes](#http-notes) | one request per message | REST APIs and webhooks of other systems |
| **Amazon SQS** | `NymBroker.Endpoint.AwsSqs` | yes | long polling | retried, then the queue's redrive dead-letter queue; messages that can never succeed go to the broker dead-letter endpoint | batches of 10 (not atomic) | AWS, managed queues (standard and FIFO) |

"Failed messages" is explained in [Reliability](reliability.md). PostgreSQL, SQL Server and MySQL let several application instances poll the same table safely; SQLite is meant for a single machine.

For the full durability, multi-node, dead-letter and crash/restart comparison, see [Delivery guarantees](delivery-guarantees.md).

## Registering endpoints in code

Every endpoint has an `Add…EndPoint(name, settings, mode)` method on the builder.

```csharp
services.AddNymBroker()
    // Core
    .AddMemoryEndPoint("Work", capacity: 1000)
    .AddFileEndPoint("Drop", new FileSettings { ReadPath = "in", PostPath = "out" })

    // NymBroker.Endpoint.Sqlite (namespace NymBroker.Endpoint.Sqlite)
    .AddSqliteEndPoint("Local", new SqliteSettings { ConnectionString = "Data Source=messages.db" })

    // NymBroker.Endpoint.Postgres
    .AddPostgresEndPoint("Pg", new PostgresSettings
    {
        ConnectionString = "Host=localhost;Database=app;Username=app;Password=…",
        TableName = "orders_queue"
    })

    // NymBroker.Endpoint.SqlServer
    .AddSqlServerEndPoint("Mssql", new SqlServerSettings
    {
        ConnectionString = "Server=…;Database=app;…",
        TableName = "dbo.orders_queue"
    })

    // NymBroker.Endpoint.MySql
    .AddMySqlEndPoint("My", new MySqlSettings
    {
        ConnectionString = "Server=…;Database=app;User ID=…;Password=…;SslMode=VerifyFull",
        TableName = "orders_queue"
    })

    // NymBroker.Endpoint.RabbitMq
    .AddRabbitMqEndPoint("Rabbit", new RabbitMqSettings
    {
        HostName = "localhost", ReadQueueName = "orders.in", WriteQueueName = "orders.in"
    })

    // NymBroker.Endpoint.AzureServiceBus
    .AddAzureServiceBusEndPoint("Bus", new AzureServiceBusSettings
    {
        ConnectionString = "<namespace connection string>",   // or FullyQualifiedNamespace + Credential
        QueueName = "orders"                                  // or TopicName (+ SubscriptionName to receive)
    })

    // NymBroker.Endpoint.Http (send only)
    .AddHttpEndPoint("PartnerHook", new HttpSettings
    {
        Url           = "https://partner.example.com/hooks/orders",
        SigningSecret = "<from a secret store>",   // adds an HMAC-SHA256 X-NymBroker-Signature header
        Headers       = { ["X-Api-Key"] = "<from a secret store>" }
    })

    // NymBroker.Endpoint.AwsSqs
    .AddAwsSqsEndPoint("Sqs", new AwsSqsSettings
    {
        QueueName = "orders",          // or QueueUrl; a name ending in .fifo is a FIFO queue
        Region    = "eu-north-1"       // credentials: the AWS SDK's default chain, or Credentials in code
    })
    .AddConsumer<OrderConsumer>()
    .Build();
```

Each endpoint's settings type defines its available options and defaults. SQL Server and PostgreSQL coordinate automatic queue schema creation between broker instances using database transaction locks; an instance may wait while another creates the tables and indexes. SQL Server serializes queue schema initialization across the database, including for different tables. With `AutoCreateTable = false`, deploy the schema beforehand instead.

The most important shared settings are:

| Setting | Endpoints | Meaning |
|---|---|---|
| `ConnectionString`, `TableName`, `AutoCreateTable` | SQLite, PostgreSQL, SQL Server, MySQL | where the queue table lives; it is created (and migrated) on first use when `AutoCreateTable` is true |
| `BatchSize`, `PollInterval` | SQL endpoints | rows claimed per poll; how long to wait after a poll that found nothing |
| `LeaseTimeout`, `MaxRetryCount` | SQL endpoints | how long a claimed row is locked; attempts before a row becomes `Failed` |
| `CompletedRetention`, `FailedRetention`, `CleanupInterval`, `CleanupBatchSize` | SQL endpoints | when finished rows are deleted (default after 30 and 90 days, checked every 10 minutes) — see [Removing old rows](#removing-old-rows) |
| `UseNotifications` | PostgreSQL | wake idle listeners with `NOTIFY` instead of waiting out `PollInterval` |
| `ReadQueueName`, `WriteQueueName` | RabbitMQ | queue to consume from / publish to |
| `PrefetchCount` | RabbitMQ | unacknowledged messages delivered at a time, default 100, 0 = no limit. A higher value gives more throughput with very fast handlers; a lower one shares a backlog better between instances. Where the server applies a consumer timeout (30 minutes by default; older servers and quorum queues), keep `PrefetchCount` × handler time below it |
| `DurablePublish` | RabbitMQ | persistent messages and publisher confirms, default `true`. Each `PostAsync` waits for a confirm, so post in batches if volume matters. `false` is much faster, but a posted message can be lost without the caller knowing |
| `UseTls`, `TlsServerName`, `ClientCertificatePath` | RabbitMQ | TLS with verified server certificate, optional client certificate — see [Production security](security.md#rabbitmq) |
| `MaxConcurrentCalls`, `PrefetchCount` | Azure Service Bus | parallel handlers (above 1 gives up ordering); messages fetched ahead |
| `ReadDeadLetterQueue` | Azure Service Bus | read the entity's dead-letter queue instead (repair / replay) |
| `WaitTimeSeconds`, `MaxNumberOfMessages`, `VisibilityTimeout` | Amazon SQS | long-poll wait (default 20 s); messages per receive (1–10); invisibility while a receive batch is handled (must cover the whole batch) |
| `RetryDelay`, `MessageGroupId` | Amazon SQS | how long a failed message stays invisible before redelivery (default 0); FIFO group (default: the envelope's correlation id) |
| `Url`, `Method`, `Headers`, `Timeout` | HTTP | where and how each message is sent; extra headers (never logged); timeout per attempt |
| `BodyFormat`, `SigningSecret` | HTTP | `Envelope` (default) or only the `Payload`; HMAC signature for the receiver to verify |
| `MaxRetryAttempts`, `RetryDelay`, `HealthCheckUrl` | HTTP | retries for transient failures (default 3, from 1 s, exponential); URL the health check GETs (unset: no probe) |
| `UseNativeDeadLetter` | RabbitMQ, Service Bus, SQL endpoints | let the transport retry and dead-letter (default `true`) — see [Reliability](reliability.md) |

The default connection settings, and the connection strings in the samples, match the local Docker containers — plaintext RabbitMQ, `TrustServerCertificate=True` for SQL Server, well-known passwords. For production, see [Production security](security.md).

Local Docker containers for every transport: [Local infrastructure](samples.md#local-infrastructure).

### Amazon SQS notes

- **Sample:** [NymBroker.AwsSqsSample](../samples/NymBroker.AwsSqsSample) shows a failed queue and a FIFO queue.
- **Failures.** SQS has no call to dead-letter a message on request. A failed message (`Retry`) becomes visible again after `RetryDelay`; give the queue a **redrive policy** (`maxReceiveCount` + a dead-letter queue), or it is retried until it succeeds or expires. Messages that can never succeed (undecodable, expired) and consumer failures go to the broker's `WithDeadLetterEndpoint`, so configure one.
- **Visibility.** Messages are deleted (one `DeleteMessageBatch`) when the whole receive batch has been handled. Set the queue's visibility timeout, or `VisibilityTimeout`, longer than handling `MaxNumberOfMessages` messages takes; otherwise a message can be delivered twice.
- **Bytes.** NymBroker envelopes are sent as text. Bytes posted with `PostRawAsync` that SQS cannot carry as text (not UTF-8, or control characters) are sent as Base64 with the message attribute `nymbroker-encoding` and decoded on receive.
- **FIFO queues** (`.fifo`): the message group is `MessageGroupId`, or else the envelope's `correlationId`; the deduplication id is the envelope's `id`, so posting the same envelope twice within SQS's deduplication window delivers it once.
- **Size.** Check the queue's maximum message size and post larger messages with `splitThresholdBytes`. `PostBatchAsync` sends up to 10 messages and 256 KB per request and is not atomic: a failed entry throws, and the messages before it stay sent.

### HTTP notes

The HTTP endpoint only **sends**: each message posted or routed to it becomes one request (`POST` by default).

- **Body and headers.** `BodyFormat = Envelope` (default) sends the NymBroker envelope as-is — right for another NymBroker instance. `Payload` sends only the envelope's `message`, for a third-party API with its own JSON. Bytes that are not an envelope (`PostRawAsync`) are sent unchanged. Every envelope also travels as headers: `X-NymBroker-Message-Id`, `-Message-Type`, `-Correlation-Id`, and `Idempotency-Key` = the message id, so the receiver can drop repeats.
- **Failures.** A 2xx response is success. Timeouts, network errors, 408, 429 and 5xx are retried `MaxRetryAttempts` times with exponential backoff (a `Retry-After` header is honoured, up to 60 s); then — and at once for any other status, such as 400 or 401 — `PostAsync` throws an `HttpRequestException` with the `StatusCode`. When the HTTP endpoint is a **route or topic destination**, that exception makes the broker return `Retry`, so the source queue redelivers the message and finally dead-letters it. A permanent failure like 400 is therefore retried until the source's retry limit (for example `MaxRetryCount` of a SQL queue) before it is dead-lettered.
- **Reliable sending (outbox).** Posting straight to the HTTP endpoint from a web request ties the request to the remote server. To survive outages, post to a durable queue and route it to the HTTP endpoint; a failed call is then retried by the queue:

  ```csharp
  .AddSqliteEndPoint("Outbox", new SqliteSettings { ConnectionString = "Data Source=outbox.db" })
  .AddHttpEndPoint("PartnerHook", new HttpSettings { Url = "https://partner.example.com/hooks/orders" })
  .AddRoute<OrderCreated>(r => r.To("PartnerHook").WhenFrom("Outbox"))
  …
  await broker.PostAsync("Outbox", order);   // durable at once; delivered (and retried) by the queue
  ```

- **HttpClient.** `AddHttpEndPoint` registers a named client (`"NymBroker.Http." + name`) with `IHttpClientFactory`. Its last parameter configures it, e.g. `client => client.AddHttpMessageHandler<MyAuthHandler>()` for OAuth tokens or a proxy.
- **Receiving webhooks** belongs in your ASP.NET Core app: verify the request (signature or API key), `await broker.PostRawAsync("Inbox", body)` to a **durable** queue, and return `202 Accepted`. Never run the consumer inside the request — the response would then depend on the consumer and its retries. Use an [input transformer](pipeline-extensions.md#input-transformers) for another system's JSON. [NymBroker.WebhookSample](../samples/NymBroker.WebhookSample) shows both directions, including signature verification with replay protection.

## Endpoint modes

Every `Add…EndPoint` takes an optional `EndpointMode`:

| Mode | Sends | Receives | Use for |
|---|---|---|---|
| `ReadWrite` (default) | ✓ | ✓ | most endpoints |
| `WriteOnly` | ✓ | — | producer processes that post and exit; dead-letter and audit endpoints; any destination you don't want processed again |
| `ReadOnly` | — | ✓ | inputs you must never post to |

```csharp
.AddSqliteEndPoint("Queue", settings, EndpointMode.WriteOnly)   // producer: no poll loop, exits promptly
```

`StartAsync` fails fast if a route, topic, dead-letter endpoint or wire tap targets a `ReadOnly` endpoint.

```mermaid
flowchart LR
    subgraph producer["Producer process"]
        P["PostAsync"] --> W["'Queue' (WriteOnly)"]
    end
    W --> DB[("orders table")]
    DB --> R
    subgraph consumer["Consumer process (one or more instances)"]
        R["'Queue' (ReadWrite)"] --> C["OrderConsumer"]
    end
```

## Configuring endpoints from a file

Endpoints (and simple topics) can come from JSON, while consumers, subscribers and routes stay in code.

**`queuesettings.json`**

```json
{
  "NymBroker": {
    "Endpoints": [
      { "Name": "Work",  "Type": "Memory" },
      { "Name": "Drop",  "Type": "File", "Config": { "readPath": "in", "postPath": "out" } },
      { "Name": "Mssql", "Type": "SqlServer", "Config": { "connectionString": "…", "tableName": "dbo.orders_queue" } },
      { "Name": "Bus",   "Type": "AzureServiceBus", "Config": { "connectionString": "…", "queueName": "orders" } },
      { "Name": "Out",   "Type": "Memory", "Mode": "WriteOnly" }
    ],
    "Topics": [
      { "TopicName": "orders.events", "MessageType": "order.created", "SubscriberEndpoints": [ "Drop" ] }
    ]
  }
}
```

```csharp
services.AddNymBroker()
    .LoadConfiguration("queuesettings.json")   // also registers File and Memory entries
    .WithSqlServer()                           // each add-on registers its own entries
    .WithAzureServiceBus()
    .WithAwsSqs()
    .WithMySql()
    .WithHttp()
    .AddConsumer<OrderConsumer>()
    .Build();
```

- `Type` is matched case-insensitively. Built-in types: `Memory`, `File`, `Sql` (SQLite), `Postgres`, `RabbitMq`; add-ons define `SqlServer`, `MySql`, `AzureServiceBus`, `AwsSqs` and `Http`. An entry is only registered if the matching `With…()` is called — otherwise it is ignored, and posting to it fails with "No endpoint registered".
- `Config` holds the endpoint's settings in camelCase.
- `Mode` is a name, case-insensitive (`"ReadWrite"`, `"ReadOnly"`, `"WriteOnly"`), or a number (`0`, `1`, `2`). Omitted means `ReadWrite`. An unknown value makes the configuration fail to load with a `JsonException`.
- Secrets such as a Service Bus `TokenCredential` cannot come from the file; register those endpoints in code.
- To read from `appsettings.json` / `IConfiguration` instead of a file: `.ApplyConfiguration(BrokerConfigurationReader.Read(configuration))`.

## Deferred messages table

Each SQL queue has a second table, `<table>_deferred`, for messages posted with [`PostDeferredAsync`](sending-messages.md#deferring-a-message). A listener checks it about four times a second. With `AutoCreateTable = true` it is created on first use. With `AutoCreateTable = false`, create it in your deployment step: without it, ordinary messages still work, but the listener logs an error every few seconds and deferred posts fail. The table for each database:

```sql
-- PostgreSQL
CREATE TABLE orders_queue_deferred (
    deferred_id    BIGSERIAL PRIMARY KEY,
    deliver_at_utc TIMESTAMPTZ NOT NULL,
    message_id     UUID        NOT NULL,
    payload        BYTEA       NOT NULL);
CREATE INDEX ix_orders_queue_deferred_due ON orders_queue_deferred (deliver_at_utc, deferred_id);

-- SQL Server
CREATE TABLE dbo.orders_queue_deferred (
    deferred_id    BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT pk_orders_queue_deferred PRIMARY KEY CLUSTERED,
    deliver_at_utc DATETIME2(7)     NOT NULL,
    message_id     UNIQUEIDENTIFIER NOT NULL,
    payload        VARBINARY(MAX)   NOT NULL);
CREATE INDEX ix_orders_queue_deferred_due ON dbo.orders_queue_deferred (deliver_at_utc);

-- MySQL
CREATE TABLE orders_queue_deferred (
    deferred_id    BIGINT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
    deliver_at_utc DATETIME(6) NOT NULL,
    message_id     BINARY(16)  NOT NULL,
    payload        LONGBLOB    NOT NULL,
    INDEX ix_due (deliver_at_utc, deferred_id)) ENGINE = InnoDB;

-- SQLite (DeliverAtUtc is in Unix milliseconds)
CREATE TABLE orders_queue_deferred (
    DeferredId   INTEGER NOT NULL PRIMARY KEY,
    DeliverAtUtc INTEGER NOT NULL,
    MessageId    BLOB    NOT NULL,
    Payload      BLOB    NOT NULL);
CREATE INDEX IX_orders_queue_deferred_Due ON orders_queue_deferred (DeliverAtUtc, DeferredId);
```

Listeners need `SELECT, DELETE` on this table, and whoever posts deferred messages needs `INSERT`. During a rolling upgrade, instances on an older version ignore the table, so they never deliver a deferred message early.

## Removing old rows

A SQL queue keeps a row after its message is handled. So that the table doesn't grow forever, a listener deletes old rows in the background:

```csharp
new SqliteSettings
{
    CompletedRetention = TimeSpan.FromDays(30),     // default
    FailedRetention    = TimeSpan.FromDays(90),     // default; how long failed rows can be inspected or replayed
    CleanupInterval    = TimeSpan.FromMinutes(10),  // default
    CleanupBatchSize   = 1000                       // default; rows per DELETE
};
```

In `queuesettings.json`, write the times as `"completedRetention": "30.00:00:00"`.

- A row's age counts from when it became `Completed` or `Failed`, by the database's clock. `Pending` and `InProgress` rows, and rows in `<table>_deferred`, are never deleted.
- `CompletedRetention` or `FailedRetention` set to `TimeSpan.Zero` keeps those rows forever. `CleanupInterval` set to `TimeSpan.Zero` turns the cleanup off.
- The first cleanup runs one `CleanupInterval` after the listener starts. Only listening endpoints clean up, so a `WriteOnly` endpoint never does. Several instances on one table are safe.
- Rows are deleted in batches, each in its own transaction, so a large backlog doesn't hold a long lock. Messages keep moving during a cleanup.
- If a cleanup fails, an error is logged and it tries again at the next interval. Messages are not affected.
- Nothing in NymBroker needs finished rows: duplicate detection has its own table. If you use these rows for reporting or auditing, keep them longer or copy them out first.
- The database doesn't always give the space back to the operating system. A SQLite file stops growing, because new rows reuse the freed pages, but it only shrinks with `VACUUM`.

With `AutoCreateTable = true` the indexes the cleanup uses are created on first use. With `AutoCreateTable = false`, create them in your deployment step. Without them the cleanup still works, but reads the whole table each time, and on SQLite holds the write lock while it does. The listener's identity also needs `DELETE` on the queue table ([Production security](security.md)). For a table named `orders_queue`:

```sql
-- PostgreSQL
CREATE INDEX ix_orders_queue_completed ON orders_queue (completed_at_utc) WHERE status = 2;
CREATE INDEX ix_orders_queue_failed    ON orders_queue (failed_at_utc)    WHERE status = 3;

-- SQL Server
CREATE INDEX ix_dbo_orders_queue_completed ON dbo.orders_queue (completed_at_utc) WHERE status = 2;
CREATE INDEX ix_dbo_orders_queue_failed    ON dbo.orders_queue (failed_at_utc)    WHERE status = 3;

-- MySQL (failed rows use the existing status index)
ALTER TABLE orders_queue ADD INDEX ix_completed_at (completed_at_utc);

-- SQLite
CREATE INDEX IX_orders_queue_Completed ON orders_queue (CompletedAtUtc) WHERE Status = 2;
CREATE INDEX IX_orders_queue_Failed    ON orders_queue (FailedAtUtc)    WHERE Status = 3;
```

To add them to a large PostgreSQL table without blocking writes, use `CREATE INDEX CONCURRENTLY` before upgrading; `AutoCreateTable` then finds them and does nothing.
