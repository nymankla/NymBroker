using NymBroker.Core.Endpoint.Queue;
using NymBroker.Shared;

namespace NymBroker.Endpoint.Postgres;

/// <summary>
/// SQL for the queue table. Status values are inlined as literals (0 Pending, 1 InProgress, 2 Completed,
/// 3 Failed) rather than passed as parameters: the planner can only use the partial "active" index when it can
/// prove at plan time that the query predicate implies the index predicate — which it cannot do for parameters
/// (and Npgsql auto-prepares statements, so generic plans are common).
/// </summary>
internal static class PostgresQueueSql
{
    internal const int Pending = (int)QueueMessageStatus.Pending;
    internal const int InProgress = (int)QueueMessageStatus.InProgress;
    internal const int Completed = (int)QueueMessageStatus.Completed;
    internal const int Failed = (int)QueueMessageStatus.Failed;

    internal static string QuoteQualifiedIdentifier(string identifier) => PostgresIdentifier.QuoteQualified(identifier);

    internal static string QuoteSimpleIdentifier(string identifier) => PostgresIdentifier.QuoteSimple(identifier);

    internal static string GetNotificationChannel(string tableName)
        => $"nymbroker_{string.Concat(tableName.Select(ch => char.IsLetterOrDigit(ch) ? char.ToLowerInvariant(ch) : '_'))}_changed";

    /// <summary>
    /// Creates the table and the partial index on Pending/InProgress rows (which keeps claiming O(batch) no matter
    /// how many rows are queued or completed), and one partial index per finished status for the retention cleanup.
    /// Also drops the two full status indexes created by NymBroker 0.1.x — the partial index replaces them, and every
    /// status change had to update both.
    /// </summary>
    internal static string CreateSchema(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        var safeName = tableName.Replace('.', '_');

        return $"""
            CREATE TABLE IF NOT EXISTS {table} (
                queue_id         BIGSERIAL PRIMARY KEY,
                message_id       UUID        NOT NULL UNIQUE,
                status           INTEGER     NOT NULL DEFAULT 0 CHECK (status IN (0, 1, 2, 3)),
                created_at_utc   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                locked_until_utc TIMESTAMPTZ NULL,
                completed_at_utc TIMESTAMPTZ NULL,
                failed_at_utc    TIMESTAMPTZ NULL,
                attempt_count    INTEGER     NOT NULL DEFAULT 0,
                last_error       TEXT        NULL,
                payload          BYTEA       NOT NULL
            );
            CREATE INDEX IF NOT EXISTS {QuoteSimpleIdentifier($"ix_{safeName}_active")}
                ON {table}(created_at_utc, queue_id)
                WHERE status IN ({Pending}, {InProgress});
            CREATE INDEX IF NOT EXISTS {QuoteSimpleIdentifier($"ix_{safeName}_completed")}
                ON {table}(completed_at_utc)
                WHERE status = {Completed};
            CREATE INDEX IF NOT EXISTS {QuoteSimpleIdentifier($"ix_{safeName}_failed")}
                ON {table}(failed_at_utc)
                WHERE status = {Failed};
            DROP INDEX IF EXISTS {QualifiedIndexName(tableName, $"ix_{safeName}_status_created")};
            DROP INDEX IF EXISTS {QualifiedIndexName(tableName, $"ix_{safeName}_status_locked_until")};
            CREATE TABLE IF NOT EXISTS {DeferredTable(tableName)} (
                deferred_id    BIGSERIAL PRIMARY KEY,
                deliver_at_utc TIMESTAMPTZ NOT NULL,
                message_id     UUID        NOT NULL,
                payload        BYTEA       NOT NULL
            );
            CREATE INDEX IF NOT EXISTS {QuoteSimpleIdentifier($"ix_{safeName}_deferred_due")}
                ON {DeferredTable(tableName)}(deliver_at_utc, deferred_id);
            """;
    }

    internal static string DeleteCompleted(string tableName) => DeleteFinished(tableName, Completed, "completed_at_utc");

    internal static string DeleteFailed(string tableName) => DeleteFinished(tableName, Failed, "failed_at_utc");

    /// <summary>
    /// One batch of old finished rows, found through the status's partial index. <c>SKIP LOCKED</c> lets several instances
    /// do this at the same time.
    /// </summary>
    private static string DeleteFinished(string tableName, int status, string finishedAtColumn)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return $"""
            DELETE FROM {table}
            WHERE queue_id IN (
                SELECT queue_id FROM {table}
                WHERE status = {status}
                  AND {finishedAtColumn} < NOW() - (@olderThanSeconds * INTERVAL '1 second')
                FOR UPDATE SKIP LOCKED
                LIMIT @batchSize)
            """;
    }

    /// <summary>
    /// Deferred messages wait in their own table, not in the queue table, so any number of them leaves the claim's
    /// index scan untouched and older versions of this package never deliver them early.
    /// </summary>
    private static string DeferredTable(string tableName) => QuoteQualifiedIdentifier($"{tableName}_deferred");

    internal static string InsertDeferred(string tableName)
        => $"INSERT INTO {DeferredTable(tableName)} (deliver_at_utc, message_id, payload) VALUES (@deliverAt, @messageId, @payload)";

    /// <summary>
    /// Moves up to <c>@batchSize</c> due messages to the queue in one statement. <c>SKIP LOCKED</c> lets several instances
    /// do this at the same time.
    /// </summary>
    internal static string PromoteDue(string tableName)
    {
        var deferred = DeferredTable(tableName);
        return $"""
            WITH due AS (
                DELETE FROM {deferred}
                WHERE deferred_id IN (
                    SELECT deferred_id FROM {deferred}
                    WHERE deliver_at_utc <= NOW()
                    ORDER BY deliver_at_utc, deferred_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT @batchSize)
                RETURNING deferred_id, deliver_at_utc, message_id, payload
            )
            INSERT INTO {QuoteQualifiedIdentifier(tableName)} (message_id, status, created_at_utc, attempt_count, payload)
            SELECT message_id, {Pending}, NOW(), 0, payload FROM due ORDER BY deliver_at_utc, deferred_id
            """;
    }

    /// <summary>One statement so all five counts are read from the same PostgreSQL snapshot.</summary>
    internal static string InspectCounts(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return $"""
            SELECT (SELECT COUNT(*) FROM {table} WHERE status = {Pending}),
                   (SELECT COUNT(*) FROM {table} WHERE status = {InProgress}),
                   (SELECT COUNT(*) FROM {table} WHERE status = {Failed}),
                   (SELECT COUNT(*) FROM {table} WHERE status = {Completed}),
                   (SELECT COUNT(*) FROM {DeferredTable(tableName)})
            """;
    }

    /// <summary>Reads one state in stable oldest-first order; cursor keys are queue or timestamp/id pairs.</summary>
    internal static string InspectList(string tableName, QueueInspectionState state)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return state switch
        {
            QueueInspectionState.Pending or QueueInspectionState.InProgress => $"""
                SELECT queue_id, 0::bigint, message_id, created_at_utc, NULL::timestamptz, NULL::timestamptz,
                       attempt_count, last_error, payload
                  FROM {table}
                 WHERE status = {(state == QueueInspectionState.Pending ? Pending : InProgress)}
                   AND queue_id > @afterId
                 ORDER BY queue_id
                 LIMIT @limit
                """,
            QueueInspectionState.Failed => $"""
                SELECT failed_at_utc, queue_id, message_id, created_at_utc, NULL::timestamptz, failed_at_utc,
                       attempt_count, last_error, payload
                  FROM {table}
                 WHERE status = {Failed}
                   AND failed_at_utc >= COALESCE(@since, '-infinity'::timestamptz)
                   AND (@afterTime IS NULL OR failed_at_utc > @afterTime OR
                        (failed_at_utc = @afterTime AND queue_id > @afterId))
                 ORDER BY failed_at_utc, queue_id
                 LIMIT @limit
                """,
            _ => $"""
                SELECT deliver_at_utc, deferred_id, message_id, deliver_at_utc, deliver_at_utc, NULL::timestamptz,
                       0, NULL::text, payload
                  FROM {DeferredTable(tableName)}
                 WHERE @afterTime IS NULL OR deliver_at_utc > @afterTime OR
                       (deliver_at_utc = @afterTime AND deferred_id > @afterId)
                 ORDER BY deliver_at_utc, deferred_id
                 LIMIT @limit
                """
        };
    }

    internal static string InsertMessage(string tableName, bool notifyListeners)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        var notify = notifyListeners
            ? $"; NOTIFY {QuoteSimpleIdentifier(GetNotificationChannel(tableName))}"
            : string.Empty;

        return $"""
            INSERT INTO {table}
                (message_id, status, created_at_utc, attempt_count, payload)
            VALUES
                (@messageId, {Pending}, NOW(), 0, @payload){notify}
            """;
    }

    /// <summary>
    /// Inserts a batch in one statement (one transaction, one NOTIFY). <c>WITH ORDINALITY ... ORDER BY</c> keeps the
    /// batch order, so queue ids — and therefore delivery order — follow it.
    /// </summary>
    internal static string InsertMessages(string tableName, bool notifyListeners)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        var notify = notifyListeners
            ? $"; NOTIFY {QuoteSimpleIdentifier(GetNotificationChannel(tableName))}"
            : string.Empty;

        return $"""
            INSERT INTO {table}
                (message_id, status, created_at_utc, attempt_count, payload)
            SELECT u.message_id, {Pending}, NOW(), 0, u.payload
            FROM unnest(@messageIds, @payloads) WITH ORDINALITY AS u(message_id, payload, ord)
            ORDER BY u.ord{notify}
            """;
    }

    /// <summary>
    /// Claims up to <c>@batchSize</c> Pending (or lease-expired) rows. <c>FOR UPDATE SKIP LOCKED</c> lets several
    /// instances poll one table; the predicate matches the partial index, so the ordered index scan stops after
    /// <c>@batchSize</c> rows instead of reading and sorting the whole backlog.
    /// </summary>
    internal static string ClaimMessages(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return $"""
            WITH claimed AS (
                SELECT queue_id
                FROM {table}
                WHERE status IN ({Pending}, {InProgress})
                  AND (status = {Pending} OR locked_until_utc <= NOW())
                ORDER BY created_at_utc, queue_id
                FOR UPDATE SKIP LOCKED
                LIMIT @batchSize
            )
            UPDATE {table} AS m
            SET status = {InProgress},
                locked_until_utc = NOW() + (@leaseTimeout * INTERVAL '1 second'),
                attempt_count = m.attempt_count + 1,
                last_error = NULL,
                failed_at_utc = NULL
            FROM claimed
            WHERE m.queue_id = claimed.queue_id
            RETURNING m.queue_id, m.message_id, m.payload, m.attempt_count
            """;
    }

    /// <summary>
    /// Writes the outcome of a processed batch in one statement (any mix of Completed / back to Pending / Failed).
    /// Only rows still InProgress with the same attempt number are updated, so a poller whose lease expired cannot
    /// overwrite a row another poller has re-claimed.
    /// </summary>
    internal static string FinalizeMessages(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return $"""
            UPDATE {table} AS target
            SET status = source.status,
                locked_until_utc = NULL,
                completed_at_utc = CASE WHEN source.status = {Completed} THEN NOW() ELSE NULL END,
                failed_at_utc = CASE WHEN source.status = {Failed} THEN NOW() ELSE NULL END,
                last_error = source.error
            FROM unnest(@queueIds, @attempts, @statuses, @errors) AS source(queue_id, attempt, status, error)
            WHERE target.queue_id = source.queue_id
              AND target.status = {InProgress}
              AND target.attempt_count = source.attempt
            """;
    }

    internal static string Listen(string tableName)
        => $"LISTEN {QuoteSimpleIdentifier(GetNotificationChannel(tableName))};";

    /// <summary>Indexes live in their table's schema, so DROP INDEX needs the same schema prefix.</summary>
    private static string QualifiedIndexName(string tableName, string indexName)
    {
        var parts = PostgresIdentifier.Split(tableName);
        return parts.Length > 1
            ? $"{QuoteSimpleIdentifier(parts[0])}.{QuoteSimpleIdentifier(indexName)}"
            : QuoteSimpleIdentifier(indexName);
    }
}
