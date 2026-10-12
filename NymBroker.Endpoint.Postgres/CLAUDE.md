# NymBroker.Endpoint.Postgres

`PostgresEndPoint` — a lease-based queue table on Npgsql, same lifecycle as SQLite (`Pending → InProgress → Completed | Failed`). A private nested `Listener : LeasedQueueListener` overrides `FinalizeAndClaimAsync`, `FinalizeAsync`, `WaitWhenIdleAsync` (LISTEN/NOTIFY) and `OnLoopExitAsync`. SQL in `PostgresQueueSql`.

- **Claim**: `SELECT … FOR UPDATE SKIP LOCKED`, so several application instances can poll one table without a process-wide lock.
- **Polling**: back to back while batches are non-empty; only after an empty poll does it wait for `NOTIFY` or `PollInterval`. (Waiting after every batch capped throughput at `BatchSize / PollInterval`.)
- **One round trip and one commit per batch**: the finalize of batch N and the claim of batch N+1 go as one `NpgsqlBatch`, which PostgreSQL runs as one implicit transaction.
- **Finalize** writes mixed outcomes in one `UPDATE … FROM unnest(@queueIds, @attempts, @statuses, @errors)`, guarded by `status = InProgress AND attempt_count = source.attempt` (an expired poller cannot overwrite a re-claimed row).
- **Schema**: one partial index `(created_at_utc, queue_id) WHERE status IN (0, 1)`. Claim predicates inline the status **literals** — the planner only uses a partial index when it can prove the predicate at plan time, which it can't with parameters (Npgsql auto-prepares). `CreateSchema` drops the old full status indexes `ix_<table>_status_created` / `ix_<table>_status_locked_until` if present.
- **LISTEN failures** (at startup or later) → `LogWarning`, fall back to timer polling, retry LISTEN on the next idle cycle. An unreachable database at startup must not kill the loop.
- **Inserts**: one autocommit `INSERT` (+ `NOTIFY`) per `PostAsync`, bound by the WAL flush. Removing `NOTIFY` was measured and gained little. `PostBatchAsync`: `INSERT … SELECT FROM unnest WITH ORDINALITY` + one `NOTIFY`, atomic and ordered.
- `DeadLetter` → `Failed` with `"{Reason}: {Description}"` in `last_error`; `LogWarning` when a message reaches `Failed`. Shutdown writes handled outcomes with a fresh 10 s token (`LeasedQueueListener`); failure → `LogWarning`, redelivered after lease expiry.

Integration tests: `PostgresEndPointTests` (`NYMBROKER_POSTGRES_CS`, `./scripts/setup-postgres.ps1`).
