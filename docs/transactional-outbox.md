# Transactional outbox and inbox

[← User guide](user-guide.md)

A consumer or a web request often changes its own database and sends a message about it. Doing the two separately leaves
a window either way:

- **Post, then commit:** the commit can fail after the message is gone, so an event goes out for a change that never happened.
- **Commit, then post:** the process can stop between the two, so the change happens and the event is lost.

On the receiving side, the [duplicate detection](reliability.md#duplicate-detection) record is also written separately
from the consumer's own changes. If the process stops after the consumer's commit and before the broker records the message,
the message is processed again when it is redelivered.

The **outbox** (`IOutbox`) closes the first gap: the message is written to a table in your own transaction and sent by a
background dispatcher after the commit. The **inbox** (`IInbox`) closes the second: the consumer records the message as
handled in its own transaction. Both work through an ADO.NET `DbTransaction`, so they fit plain ADO.NET and EF Core (`db.Database.CurrentTransaction.GetDbTransaction()`).

Neither makes delivery exactly once. Read [Guarantees](#guarantees) before relying on them.

## Packages

| Package | Builder method | Transaction type |
|---|---|---|
| `NymBroker.Outbox.SqlServer` | `AddSqlServerOutbox` | `SqlTransaction` |
| `NymBroker.Outbox.Postgres` | `AddPostgresOutbox` | `NpgsqlTransaction` |
| `NymBroker.Outbox.MySql` | `AddMySqlOutbox` (MySQL 8.0+, MariaDB 10.6+) | `MySqlTransaction` |
| `NymBroker.Outbox.Sqlite` | `AddSqliteOutbox` (one host) | `SqliteTransaction` |

The inbox needs no extra package: every `NymBroker.Idempotency.*` store can join a transaction.

## Outbox

```csharp
// dotnet add package NymBroker.Outbox.SqlServer
services.AddNymBroker()
    .AddRabbitMqEndPoint("OrderEvents", rabbitSettings, EndpointMode.WriteOnly)
    .AddSqlServerOutbox(new SqlServerOutboxSettings
    {
        ConnectionString = "...",                 // the database your transactions are on
        TableName        = "dbo.nymbroker_outbox" // created automatically (AutoCreateTable = true)
    }, o =>
    {
        o.PollInterval = TimeSpan.FromSeconds(1); // how soon a committed message is sent (default 1 s)
        o.MaxAttempts  = 10;                       // then the row is kept as failed
    })
    .Build();
```

Inject `IOutbox` and post in your transaction:

```csharp
public sealed class OrderService(OrdersDbContext db, IOutbox outbox)
{
    public async Task PlaceAsync(Order order, CancellationToken ct)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        db.Orders.Add(order);
        await db.SaveChangesAsync(ct);

        await outbox.PostAsync("OrderEvents", new OrderPlaced(order.Id), tx.GetDbTransaction(), ct);

        await tx.CommitAsync(ct);   // the order and the event, or neither
    }
}
```

With ADO.NET, pass the `SqlTransaction` (or `NpgsqlTransaction`, ...) itself. `IOutbox` has:

| Method | Like |
|---|---|
| `PostAsync<T>(endpoint, message, transaction, ct)` | `INymBroker.PostAsync` |
| `PostDeferredAsync<T>(endpoint, message, deliverAt, transaction, ct)` | `INymBroker.PostDeferredAsync`: the message waits **in the outbox table** until it is due, so the wait is durable for every endpoint, including those that cannot defer |
| `PostRawAsync(endpoint, bytes, transaction, ct)` | `INymBroker.PostRawAsync` |

The envelope, and so the message id, is created by the call. An unknown or read-only endpoint, or a byte array posted as a
typed message, throws at once, before anything is written. Output transformers run when the message is sent. Splitting
(`splitThresholdBytes`) is not available through the outbox.

### The dispatcher

A hosted service, started after the broker and stopped before it, runs on every instance:

1. Claims up to `BatchSize` (100) committed, due rows, oldest first, with a lease of `LeaseTimeout` (1 minute). Several
   instances share the table: each row is claimed by one of them at a time.
2. Sends them one by one, in order, through the broker (output transformer, then the endpoint's `PostAsync`).
3. Deletes each row once it is sent.

When a batch was full it goes on at once; otherwise it waits `PollInterval`. A failed send is retried after `RetryDelay`
(1 s), doubled after each further failure up to `MaxRetryDelay` (5 min). After `MaxAttempts` (10), the row is kept with
status 2 (failed) and its last error, and the dispatcher logs an error. A row for an endpoint that is no longer registered
is marked failed at once. A failure to read the table is logged and tried again after 5 seconds.

On shutdown, rows that were claimed but not sent are handed back for the next claim, so they are not held until the lease
expires.

### Table

| Column | |
|---|---|
| `id` | Identity, insert order |
| `endpoint_name`, `body` | Where to and what: the envelope bytes |
| `deliver_at_utc` | Due time of a deferred post, else null |
| `status` | 1 pending, 2 failed |
| `attempts` | Claims so far |
| `available_at_utc` | When the row may next be claimed: its due time, then the end of a lease or a retry delay |
| `claim_id` | The current claim. Writes after a send name it, so a dispatcher whose lease expired changes nothing |
| `last_error` | The last send failure |
| `created_at_utc` | Insert time |

SQLite stores the times as Unix milliseconds. Sent rows are deleted, so the table holds only messages not yet sent and
failed ones. Failed rows stay until you deal with them. To send them again:

```sql
UPDATE dbo.nymbroker_outbox SET status = 1, attempts = 0, available_at_utc = SYSUTCDATETIME() WHERE status = 2;
```

`AutoCreateTable = true` (the default) creates the table and its index on first use; it never changes an existing table.
With `AutoCreateTable = false`, create the table with the store's schema (the SQL is in each package's `*OutboxSql.CreateSchema`)
through your own migrations.

## Inbox

The broker's [idempotent receiver](reliability.md#duplicate-detection) claims a message id before the consumer runs, and
records it as handled after the consumer returns. Inject `IInbox` and record it yourself, in the consumer's transaction:

```csharp
services.AddNymBroker()
    .AddSqlServerIdempotency(new SqlServerIdempotencySettings { ConnectionString = "...", TableName = "dbo.nymbroker_idempotency" })
    .AddConsumer<PlaceOrderConsumer>()
    .Build();

public sealed class PlaceOrderConsumer(OrdersDbContext db, IInbox inbox) : IConsume<PlaceOrder>
{
    public async Task ConsumeAsync(PlaceOrder message, IMessageContext context, CancellationToken ct = default)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        // ... the consumer's writes ...
        await inbox.MarkHandledAsync(context, tx.GetDbTransaction(), ct);
        await tx.CommitAsync(ct);   // the writes and the "handled" record, or neither
    }
}
```

The idempotency table must be in the database the transaction is on. If the transaction rolls back, the record does too,
and the broker releases its claim as usual. The broker still records the message itself after the consumer returns; that
second write changes nothing. `MarkHandledAsync` throws `InvalidOperationException` without an idempotent receiver, or with
one whose store cannot join a transaction (the in-memory store, or a custom store that does not implement
`ITransactionalIdempotencyStore`). A message without an id is not checked by the broker, so nothing is written for it.

A consumer can use both: post its follow-up events through `IOutbox` and record the message through `IInbox` in the same
transaction. The [EF Core transaction sample](../samples/NymBroker.EfCoreTransactionSample/README.md) does.

## Guarantees

| Situation | What happens |
|---|---|
| The transaction rolls back | Nothing is sent; nothing is recorded as handled. |
| The process stops after the commit, before the dispatcher sends | The message is sent when a dispatcher next runs (on any instance). |
| A dispatcher stops after a send, before it deletes the row | The lease expires and the message is sent **again, with the same envelope id**. |
| The endpoint is down | Retried with backoff, then kept as failed and logged. |
| Two instances dispatch | Each row is claimed by one at a time. Rows of transactions still open are skipped, not waited for (SQL Server `READPAST`, PostgreSQL and MySQL `SKIP LOCKED`). |
| A consumer that used `IInbox` stops after its commit | The redelivery is dropped as a duplicate. Without `IInbox` it is processed again. |

**At least once, not exactly once.** The outbox removes lost and phantom messages. It does not remove duplicates: a crash
between the send and the delete, or a send that succeeded but reported an error, sends the message again. Give the
receiving side an idempotent receiver, which drops the repeat by its envelope id. The inbox removes the window on the
receiving side for writes in the same database. A consumer's side effects elsewhere (an HTTP call, an email) can still
repeat.

**Ordering.** One dispatcher sends a batch in insert order, so messages from one instance usually arrive in order. That is
not a guarantee: a retried message is sent after later ones, several instances send in parallel, and an earlier transaction
can commit after a later one. Make receivers tolerate reordering, as for any NymBroker transport.

**Latency.** A committed message is sent within about `PollInterval`, unless the dispatcher is busy or the endpoint fails.

**SQLite.** One writer at a time: while an application transaction is open, the dispatcher's claim waits for it (up to the
busy timeout of 5 seconds, then it tries again later). The database must be a file shared by the application's connection
and the dispatcher's, on one host. In-memory databases are rejected.

## Custom stores

`IOutboxStore` is the table's contract: `AddAsync` in the caller's transaction, `ClaimAsync` with a lease, then
`MarkSentAsync`, `RetryLaterAsync`, `MarkFailedAsync` or `ReleaseAsync` naming the claim. Register an implementation with
`NymBrokerBuilder.AddOutboxStore(sp => new MyStore(...), o => ...)`; it adds `IOutbox` and the dispatcher. For the inbox,
implement `ITransactionalIdempotencyStore.CompleteInTransactionAsync` on your idempotency store.
