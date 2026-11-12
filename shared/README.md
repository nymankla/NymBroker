# Shared source

Internal source files compiled into several packages (see `Directory.Build.targets`), so the packages share code without
a shared package or public API. Everything here must stay `internal`. The types have the same full name in each
assembly, so tests reach them through a forwarding member on the package's own SQL class.

| Folder | Compiled into |
|---|---|
| `Postgres`, `SqlServer`, `MySql` | every project for that database |
| `Stores/Postgres`, `Stores/Sqlite`, `Stores/Pooled` | the idempotency, schedule and outbox stores for that database (`Pooled`: SQL Server and MySQL) |
| `Idempotency` | every idempotency store (`TransactionalCompletion`: `CompleteInTransactionAsync` for `IInbox`) |
| `Outbox` | every outbox store (`OutboxStoreCore`: insert in the caller's transaction, claim and outcome writes; `OutboxDialect`: per-database claim ids, times and claim) |
| `Inspection` | the four queue-table endpoints (`IQueueInspector`: argument checks, cursor format, counts, page loop; the SQL and parameter binding stay in each endpoint). Every endpoint's `InspectCounts` must return the columns in the order pending, in progress, failed, completed, deferred |
