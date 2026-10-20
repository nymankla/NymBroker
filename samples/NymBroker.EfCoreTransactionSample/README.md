# NymBroker.EfCoreTransactionSample

A consumer that places an order with Entity Framework Core — the order, its lines and the products' stock, three tables in one SQL Server transaction — reading from a NymBroker SQL Server queue, with the broker's SQL Server idempotency store dropping redelivered duplicates. It shows how to make a consumer safe under retries and redelivery: commit or roll back as one unit, record a business rejection instead of retrying it, and let the broker filter messages that were already handled.

## Run

```powershell
./scripts/setup-sqlserver.ps1                                      # SQL Server container on localhost,1433
dotnet run --project samples/NymBroker.EfCoreTransactionSample      # runs three scenarios, checks them, exits 0 or 1
```

The output includes every SQL statement EF Core runs, with parameter values (`EnableSensitiveDataLogging` — sample only), and the transaction steps at `Debug`: `Began transaction`, the `INSERT`s into `Orders` and `OrderLines`, the `UPDATE` of `Products`, then `Committing transaction` — or, in scenario 3, `Rolling back to transaction savepoint`, the `INSERT` into `OrderRejections`, and `Committing transaction`. (EF Core also creates a savepoint before each `SaveChanges` inside a transaction; those lines are its own.) The queue endpoint's and the idempotency store's own SQL is not EF Core and is not logged.

It uses a throw-away database `nymbroker_ef_sample` (dropped and recreated on every run). Set `NYMBROKER_SQLSERVER_CS` to use another server. The default connection string is the local container's; `TrustServerCertificate=True` is for local development only — see [Production security](../../docs/security.md#sql-server).

| Scenario | What happens | Checked |
|---|---|---|
| 1. Normal order (2 × WIDGET) | Order, line and stock change in one transaction | order + line exist, WIDGET 10 → 8 |
| 2. Same message delivered twice (1 × WIDGET) | The broker drops the second delivery before the consumer runs ("Dropping duplicate message …") | order exists once, WIDGET 8 → 7 (not 6) |
| 3. Out of stock on the second line (3 × WIDGET, 5 × GADGET with 1 in stock) | Order, lines and WIDGET's reservation are already written when GADGET runs out; the consumer rolls back to a savepoint, writes an `OrderRejections` row instead, commits, and returns normally | no order, no lines, WIDGET still 7, GADGET still 1, a rejection with the reason, the queue row `Completed` on its first attempt, the message id remembered |

## How it works

[`Program.cs`](Program.cs) registers the queue, the idempotency store and the consumer:

```csharp
builder.Services.AddDbContext<OrdersDbContext>(o => o.UseSqlServer(connectionString));   // scoped

builder.Services.AddNymBroker()
    .AddSqlServerEndPoint("Orders", new SqlServerSettings { ConnectionString = connectionString, TableName = "dbo.ef_sample_queue", MaxRetryCount = 3 })
    .AddSqlServerIdempotency(new SqlServerIdempotencySettings { ConnectionString = connectionString, TableName = "dbo.ef_sample_idempotency" })
    .AddConsumer<PlaceOrderConsumer>()
    .Build();
```

[`Orders.cs`](Orders.cs), `PlaceOrderConsumer`:

```csharp
public async Task ConsumeAsync(PlaceOrder message, IMessageContext context, CancellationToken ct = default)
{
    await using var tx = await db.Database.BeginTransactionAsync(ct);
    await tx.CreateSavepointAsync("BeforeOrder", ct);

    db.Orders.Add(new Order { OrderId = …, Lines = [ … ] });   // tables 1 and 2
    await db.SaveChangesAsync(ct);                              // written, not durable yet

    foreach (var line in message.Lines)                         // table 3
    {
        var reserved = await db.Products
            .Where(p => p.ProductId == line.ProductId && p.Stock >= line.Quantity)
            .ExecuteUpdateAsync(p => p.SetProperty(x => x.Stock, x => x.Stock - line.Quantity), ct);
        if (reserved == 0)
        {
            await tx.RollbackToSavepointAsync("BeforeOrder", ct);   // undo order, lines, earlier reservations
            db.ChangeTracker.Clear();
            db.OrderRejections.Add(new OrderRejection { OrderId = …, Reason = "not enough stock of …" });
            await db.SaveChangesAsync(ct);
            await tx.CommitAsync(ct);                               // the rejection instead of the order
            return;                                                 // Completed: no retry, no dead letter
        }
    }

    await tx.CommitAsync(ct);                                   // all three tables change together
}
```

The stock `UPDATE` only matches while enough stock is left, so two orders racing for the last items cannot both get them; `ExecuteUpdateAsync` runs inside the same transaction as `SaveChangesAsync`.

**One DbContext per message.** The broker creates a DI scope for every message it dispatches, and `AddDbContext` registers the context as scoped. So the consumer — and every scoped service it depends on — shares one context, one connection and one transaction for that message, and the scope disposes them afterwards.

**Reject, don't retry, a business outcome.** Out of stock is not a malfunction: retrying would fail again, and the dead-letter path is for messages that could not be handled at all. So the consumer rolls back to the savepoint taken before the order was written — undoing the order, its lines and the stock already reserved for earlier lines — writes an `OrderRejections` row in the same transaction, commits, and returns. The rejection is stored if and only if the order is not, the message completes on its first attempt, and the broker remembers its id, so a redelivery is dropped instead of being rejected twice. Use the rejection row (or an event published from it) to tell the customer or reorder stock.

**Throw to roll back and retry.** Anything unexpected that throws before `CommitAsync` — a deadlock, a lost connection, a bug — disposes the uncommitted transaction, which rolls everything back, and the broker returns `Retry`. The SQL Server queue puts the message back to `Pending` and redelivers it, up to `MaxRetryCount` attempts, then marks the row `Failed` with the exception message in `last_error`; the idempotency claim is released, so a replay of the `Failed` row is processed. Don't catch exceptions to "handle" them unless the message really is done — a swallowed exception completes the message and its work is lost.

**The idempotency store drops redeliveries.** The queue's own bookkeeping (marking the row `Completed`) is a separate write, so a message can be delivered again after it was handled — after a crash, an expired lease, or with RabbitMQ / Service Bus a lost ack. Before the consumer runs, the broker claims the message id in `dbo.ef_sample_idempotency`:

- new id → the consumer runs; on success the id is remembered for the `Ttl` (24 h by default), on `Retry` the claim is released so the redelivery is processed;
- id already handled → the message is completed without calling the consumer;
- id claimed by a delivery still running → `Retry`, processed once the other delivery has finished.

The table is shared by every instance of the app, and a hosted service deletes expired ids. The consumer needs no duplicate handling of its own.

The store's write is separate from your transaction: if the process dies after `CommitAsync` and before the broker records the id, the claim expires (`LeaseTimeout`, 5 min) and the message is processed again. If that window matters for a consumer, give its writes a natural key (as `OrderId` is here — a second insert would fail on the primary key, rolling back the whole transaction including the stock update) or make them upserts.

## Variations

- **Several consumers / services in one transaction:** they all resolve the same scoped `DbContext` for the message, so `SaveChangesAsync` and `CommitAsync` cover all of them.
- **Sending messages from the consumer:** `broker.PostAsync` is not part of the transaction. If a follow-up message must be sent if and only if the data is committed, write it to an outbox table in the same transaction and post it from a background job.
- **EF Core retry on failure:** with `UseSqlServer(…, o => o.EnableRetryOnFailure())`, a transaction you start yourself must run inside `db.Database.CreateExecutionStrategy().ExecuteAsync(…)`.
- **PostgreSQL / SQLite:** the same pattern with `UseNpgsql` / `UseSqlite` and `AddPostgresIdempotency` / `AddSqliteIdempotency`.
