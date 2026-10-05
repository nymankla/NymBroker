namespace NymBroker.Postgres;

/// <summary>
/// SQL for the queue table. Status values are inlined as literals (0 Pending, 1 InProgress, 2 Completed,
/// 3 Failed) rather than passed as parameters: the planner can only use the partial "active" index when it can
/// prove at plan time that the query predicate implies the index predicate — which it cannot do for parameters
/// (and Npgsql auto-prepares statements, so generic plans are common).
/// </summary>
internal static class PostgresQueueSql
{
    internal const int Pending = 0;
    internal const int InProgress = 1;
    internal const int Completed = 2;
    internal const int Failed = 3;

    internal static string QuoteQualifiedIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new InvalidOperationException("PostgreSQL identifier cannot be empty.");

        return string.Join('.', SplitIdentifier(identifier).Select(QuoteSimpleIdentifier));
    }

    internal static string QuoteSimpleIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new InvalidOperationException("PostgreSQL identifier cannot be empty.");

        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }

    internal static string GetNotificationChannel(string tableName)
        => $"nymbroker_{string.Concat(tableName.Select(ch => char.IsLetterOrDigit(ch) ? char.ToLowerInvariant(ch) : '_'))}_changed";

    /// <summary>
    /// Creates the table and the partial index on Pending/InProgress rows (which keeps claiming O(batch) no matter
    /// how many rows are queued or completed). Also drops the two full status indexes created by NymBroker 0.1.x —
    /// the partial index replaces them, and every status change had to update both.
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
            DROP INDEX IF EXISTS {QualifiedIndexName(tableName, $"ix_{safeName}_status_created")};
            DROP INDEX IF EXISTS {QualifiedIndexName(tableName, $"ix_{safeName}_status_locked_until")};
            """;
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

    private static string[] SplitIdentifier(string identifier)
        => identifier.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    /// <summary>Indexes live in their table's schema, so DROP INDEX needs the same schema prefix.</summary>
    private static string QualifiedIndexName(string tableName, string indexName)
    {
        var parts = SplitIdentifier(tableName);
        return parts.Length > 1
            ? $"{QuoteSimpleIdentifier(parts[0])}.{QuoteSimpleIdentifier(indexName)}"
            : QuoteSimpleIdentifier(indexName);
    }
}
