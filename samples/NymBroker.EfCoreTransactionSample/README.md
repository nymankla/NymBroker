# NymBroker.EfCoreTransactionSample

A consumer that places an order with Entity Framework Core — the order, its lines and the products' stock, three tables in one SQL Server transaction — reading from a NymBroker SQL Server queue, with the broker's SQL Server idempotency store dropping redelivered duplicates. It shows how to make a consumer safe under retries and redelivery: commit or roll back as one unit, and let the broker filter messages that were already handled.

## Run

```powershell
./scripts/setup-sqlserver.ps1                                      # SQL Server container on localhost,1433
dotnet run --project samples/NymBroker.EfCoreTransactionSample      # runs three scenarios, checks them, exits 0 or 1
```

The output includes every SQL statement EF Core runs, with parameter values (`EnableSensitiveDataLogging` — sample only), and the transaction steps at `Debug`: `Began transaction`, the `INSERT`s into `Orders` and `OrderLines`, the `UPDATE` of `Products`, then `Committing transaction` — or, in scenario 3, `Disposing transaction` without a commit, which is the rollback. The queue endpoint's and the idempotency store's own SQL is not EF Core and is not logged.

It uses a throw-away database `nymbroker_ef_sample` (dropped and recreated on every run). Set `NYMBROKER_SQLSERVER_CS` to use another server. The default connection string is the local container's; `TrustServerCertificate=True` is for local development only — see [Production security](../../docs/security.md#sql-server).

| Scenario | What happens | Checked |
|---|---|---|
| 1. Normal order (2 × WIDGET) | Order, line and stock change in one transaction | order + line exist, WIDGET 10 → 8 |
| 2. Same message delivered twice (1 × WIDGET) | The broker drops the second delivery before the consumer runs ("Dropping duplicate message …") | order exists once, WIDGET 8 → 7 (not 6) |
| 3. Out of stock on the second line (3 × WIDGET, 5 × GADGET with 1 in stock) | Order, lines and WIDGET's reservation are already written when GADGET fails; the transaction rolls all of it back; the claim is released; the queue retries 3 times, then marks the row `Failed` | no order, no lines, WIDGET still 7, GADGET still 1, `Failed` row with the reason |

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

    db.Orders.Add(new Order { OrderId = …, Lines = [ … ] });   // tables 1 and 2
    await db.SaveChangesAsync(ct);                              // written, not durable yet

    foreach (var line in message.Lines)                         // table 3
    {
        var reserved = await db.Products
            .Where(p => p.ProductId == line.ProductId && p.Stock >= line.Quantity)
            .ExecuteUpdateAsync(p => p.SetProperty(x => x.Stock, x => x.Stock - line.Quantity), ct);
        if (reserved == 0)
            throw new InvalidOperationException("not enough stock");   // rolls back everything above
    }

    await tx.CommitAsync(ct);                                   // all three tables change together
}
```

The stock `UPDATE` only matches while enough stock is left, so two orders racing for the last items cannot both get them; `ExecuteUpdateAsync` runs inside the same transaction as `SaveChangesAsync`.

**One DbContext per message.** The broker creates a DI scope for every message it dispatches, and `AddDbContext` registers the context as scoped. So the consumer — and every scoped service it depends on — shares one context, one connection and one transaction for that message, and the scope disposes them afterwards.

**Throw to roll back.** If anything throws before `CommitAsync` — here the out-of-stock check, after the order, its lines and earlier stock reservations were already written — disposing the uncommitted transaction rolls all of it back, and the broker returns `Retry`. The SQL Server queue puts the message back to `Pending` and redelivers it, up to `MaxRetryCount` attempts, then marks the row `Failed` with the exception message in `last_error`. Don't catch exceptions to "handle" them in the consumer unless the message really is done — a swallowed exception completes the message and its work is lost.

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
