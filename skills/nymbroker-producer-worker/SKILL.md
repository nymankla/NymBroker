---
name: nymbroker-producer-worker
description: Split an application into NymBroker producers (web API, CLI, function — WriteOnly endpoints) and one or more worker services that consume a shared durable queue, with a shared contracts project, multi-instance-safe transport, durable duplicate detection, leases and graceful shutdown. Use when the user wants a background worker, separate producer and consumer processes, to scale out consumers, run several instances, or offload work from a web API with NymBroker.
---

# NymBroker producer and worker processes

```
Web API / CLI (producer)          Worker service (1..n instances)
  PostAsync ──► "Orders" (WriteOnly) ──► [ durable queue ] ──► "Orders" (ReadWrite) ──► IConsume<OrderCreated>
```

Docs: https://github.com/nymankla/NymBroker/blob/master/docs/endpoints-and-configuration.md · https://github.com/nymankla/NymBroker/blob/master/docs/delivery-guarantees.md

## 1. Ask first

Before creating projects, confirm with the user:

1. **Transport** shared by both sides. Several worker instances need **PostgreSQL, SQL Server, RabbitMQ or Azure Service Bus**. SQLite only works when producer and worker run on the same machine (one file); Memory and File don't work across processes (File: only one consumer per folder).
2. **Projects**: which existing project is the producer, whether to create a new worker project (`dotnet new worker`), and where the shared message types go (an existing `*.Contracts` project, or a new class library both reference).
3. **How many worker instances** and whether processing order matters (NymBroker does not guarantee order across retries and parallel consumers).
4. **Duplicate detection** across instances: durable store in PostgreSQL / SQL Server (recommended for more than one instance), or idempotent consumers only.

## 2. Contracts

Put message types in the shared class library (reference only the `NymBroker` package there, for `[MessageName]`). Use the **nymbroker-message** skill. Both sides must use the same `[MessageName]` values.

## 3. Producer

```csharp
// Program.cs of the web API
builder.Services.AddNymBroker()
    .AddPostgresEndPoint("Orders", new PostgresSettings
    {
        ConnectionString = builder.Configuration.GetConnectionString("Queue")!,
        TableName        = "orders_queue",
        AutoCreateTable  = true
    }, EndpointMode.WriteOnly)          // no listener: this process only posts
    .Build();

app.MapPost("/orders", async (PlaceOrder request, INymBroker broker, CancellationToken ct) =>
{
    await broker.PostAsync("Orders", new OrderCreated(request.Id, request.CustomerId, request.Amount, DateTimeOffset.UtcNow), ct);
    return Results.Accepted();
});
```

- `WriteOnly` means no poll loop or subscription in the producer, and a short-lived process (CLI, function) exits as soon as it has posted.
- `PostAsync` returns once the transport has accepted the message: the row is committed (SQL endpoints) or the send completed (Service Bus). RabbitMQ publishes without publisher confirms, so a broker crash right after can lose it — prefer a SQL transport or Service Bus when every message counts. Return `202 Accepted`, not the result of the work.
- For many messages at once use `PostBatchAsync` (one round trip on the SQL endpoints and Service Bus).
- Posting and your own database write are not one transaction. If both must happen together, write to your database first and post after commit, and make the consumer tolerate a missing or duplicate message — or store an outbox row in the same transaction and post it from a background job.

## 4. Worker

```csharp
// Program.cs of the worker (dotnet new worker; remove the template's Worker class if unused)
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddNymBroker()
    .AddPostgresEndPoint("Orders", new PostgresSettings
    {
        ConnectionString = builder.Configuration.GetConnectionString("Queue")!,
        TableName        = "orders_queue",
        AutoCreateTable  = true,
        BatchSize        = 20,
        LeaseTimeout     = TimeSpan.FromMinutes(5)    // longer than the slowest handler
    })
    .AddPostgresIdempotency(new PostgresIdempotencySettings
    {
        ConnectionString = builder.Configuration.GetConnectionString("Queue")!
    })
    .AddConsumer<OrderCreatedConsumer>()
    .Build();

builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(30));

await builder.Build().RunAsync();
```

- The broker starts with the host and listens on every non-`WriteOnly` endpoint. Create consumers with the **nymbroker-consumer** skill.
- **Leases** (SQL endpoints): a claimed row is locked for `LeaseTimeout`. If a handler can run longer, raise it — otherwise another instance may process the same message concurrently.
- **Scaling**: PostgreSQL/SQL Server claim with `SKIP LOCKED` / `READPAST`, so instances share the table safely. Raise `BatchSize` for throughput. Service Bus: `MaxConcurrentCalls` > 1 processes in parallel within one instance (no ordering). RabbitMQ: competing consumers on one queue.
- **Shutdown**: on stop, endpoints finish in-flight messages and settle them; claimed-but-unstarted ones return after the lease/lock expires. Give the host enough `ShutdownTimeout`, and pass the `CancellationToken` through in consumers.
- **Large messages**: don't use `splitThresholdBytes` with several worker instances — parts are reassembled in memory by one instance and may be claimed by different ones.
- **Scheduled jobs** run in every instance; see the nymbroker-scheduled skill.

## 5. Operations

- Add health checks and metrics to the worker (**nymbroker-observability**); in containers, use the health endpoint as readiness probe.
- Backlog is not measured by the broker: watch the transport (rows with `status = 0`, queue length).
- Failures: SQL tables keep `Failed` rows; see **nymbroker-dead-letters**.

## 6. Finish

Build both projects, start the worker and the producer, post one message and show it being consumed. Summarize the topology for the user: which process writes, which reads, the transport, the table/queue, and the settings that matter when scaling out (lease, batch size, idempotency store).
