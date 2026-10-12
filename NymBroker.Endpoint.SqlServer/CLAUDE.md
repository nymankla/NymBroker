# NymBroker.Endpoint.SqlServer

`SqlServerEndPoint` — same lifecycle, leases, retry/`Failed` handling, logging and loop structure as `PostgresEndPoint` (private `Listener : LeasedQueueListener`), with SQL written for SQL Server (`SqlServerQueueSql`). `Microsoft.Data.SqlClient`; config type `SqlServerEndPointType.SqlServer` (defined here, not in Core). Each choice below was measured against a straight port of the Postgres SQL.

- **One round trip and one commit per batch** (`FinalizeAndClaim`): results of batch N and the claim of batch N+1 in one T-SQL batch. The transaction is in the SQL text (`SET XACT_ABORT ON; BEGIN TRANSACTION … COMMIT`), not `SqlConnection.BeginTransaction`, which costs two extra round trips.
- **Claim**: updatable CTE with `WITH (UPDLOCK, READPAST, ROWLOCK)` (SQL Server's `SKIP LOCKED`) and `OUTPUT inserted.*`, ordered by `queue_id`.
- **Finalize** reads `{id, attempt, status, error}` from one JSON `NVARCHAR(MAX)` parameter via `OPENJSON` (SQL Server 2016+). It must drive the join — `FROM OPENJSON(...) INNER LOOP JOIN <table> WITH (FORCESEEK)` — because the optimizer can't estimate `OPENJSON`'s row count and otherwise scans the table (~8× CPU). Guard: `status = InProgress AND attempt_count = source.attempt`.
- **Schema**: clustered PK on IDENTITY `queue_id`; one **filtered index** `(queue_id) INCLUDE (status, locked_until_utc) WHERE status IN (0, 1)`, so claiming stays cheap as finished rows pile up. No index on `message_id` (nobody looks it up; a random-GUID index scatters page writes). Status values are **inlined literals** — a filtered index is only used when the predicate matches at compile time.
- **Polling**: back to back while batches are non-empty; `PollInterval` only after an empty poll. No LISTEN/NOTIFY equivalent.
- **Inserts**: one autocommit `INSERT` per `PostAsync` (durable on return), bound by the log flush; concurrent producers get SQL Server's group commit. **Never use `DELAYED_DURABILITY`** for the queue — it can lose committed messages. `PostBatchAsync`: `INSERT … SELECT FROM OPENJSON` (base64 → `VARBINARY`), atomic and ordered.
- **Connections**: one pooled `SqlConnection` per operation (not thread-safe).
- Shutdown writes handled outcomes with a fresh 10 s token; claimed-but-unhandled messages wait for lease expiry.

Integration tests: `SqlServerEndPointTests` (`NYMBROKER_SQLSERVER_CS`, `./scripts/setup-sqlserver.ps1`). Production TLS / `TrustServerCertificate`: [docs/security.md](../docs/security.md).
