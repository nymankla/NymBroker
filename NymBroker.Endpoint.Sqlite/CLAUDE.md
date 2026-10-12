# NymBroker.Endpoint.Sqlite

`SqliteEndPoint` — a lease-based queue in a SQLite table. A private nested `Listener : LeasedQueueListener` holds the loop; this project only has the data access (SQL in `SqliteQueueSql`). Native `Microsoft.Data.Sqlite`, **no Dapper** (don't re-add it). Non-interface `ReadAsync` drains available rows (tests/samples; finalizes per item, decodes to strings).

Lifecycle: `Pending (0)` → `InProgress (1)` → `Completed (2)` | `Failed (3)`.

- **One connection**, serialized by `SemaphoreSlim(1,1)` (`_dbLock`) — `SqliteConnection` is not safe for concurrent use. The lock is never held while a handler runs. Every statement is **prepared once** (`PreparedCommands`) and only rebound; `SetTransaction` attaches them to the current transaction. In-memory databases (`Data Source=:memory:`) need this single persistent connection — `EnsureConnectionAsync` opens it lazily under the lock.
- **PRAGMAs on open**: `journal_mode` (default WAL) and `synchronous` (default NORMAL: survives an app crash, the last commits can be lost on power loss; `Full` for strict durability) on file databases only; `busy_timeout` (5 s) and `temp_store = MEMORY` always. Write transactions are `BEGIN IMMEDIATE`, so several processes on one file wait for the write lock instead of failing an upgrade. WAL + NORMAL is most of the insert throughput.
- **Claim** = one `UPDATE … WHERE QueueId IN (SELECT … ORDER BY QueueId LIMIT $batchSize) RETURNING …` (SQLite ≥ 3.35). The subquery repeats the partial index's `Status IN (0, 1)` **verbatim as literals** (never parameters) so the planner uses `IX_<table>_Active` — `Claim_UsesThePartialActiveIndex` checks the plan. Rows are sorted by `QueueId` after `RETURNING`.
- **Finalize** of batch N is written in the same transaction as the claim of batch N+1: one commit per batch. Each update is guarded by `Status = 1 AND AttemptCount = $attempt`, so a poller whose lease expired cannot overwrite a re-claimed row. A failed write rolls back, logs `LogError`, and keeps the outcomes for the next attempt.
- **Polling**: back to back while claims return rows; `PollInterval` (min 1 ms) only after an empty poll or an error.
- **Retry**: `AttemptCount` increments on every claim; `Retry` → `Pending` until `MaxRetryCount`, then `Failed`; `DeadLetter` → `Failed` at once with `"{Reason}: {Description}"` in `LastError`. `LeaseTimeout` controls how long a claim holds.
- **Payloads are bytes** (`BLOB`), unchanged.

**Schema v2**: `QueueId INTEGER PRIMARY KEY` (no `AUTOINCREMENT` — avoids a `sqlite_sequence` write per insert), `MessageId BLOB` (16-byte GUID, not unique, no index — only logged), `Payload BLOB`, one partial index `(QueueId) WHERE Status IN (0, 1)`. `TableName` must match `^[A-Za-z_][A-Za-z0-9_]*$` (constructor throws) and is always quoted.

**Migration**: `EnsureSchemaAsync` detects the version **per table** from `pragma_table_info` (`PRAGMA user_version` is per file and one file can hold several endpoint tables; it is still set to 2). A v1 table (TEXT payload, `MessageId TEXT UNIQUE`, `AUTOINCREMENT`) or the legacy single-status table is renamed to `<table>_v1_<timestamp>` / `<table>_Legacy_<timestamp>` (kept as backup), the new table created and rows copied (`CAST(… AS BLOB)` keeps the UTF-8 bytes) in one `BEGIN IMMEDIATE` transaction.

Config type name: `"Sql"` (`WithSql()`).
