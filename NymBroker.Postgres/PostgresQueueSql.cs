namespace NymBroker.Postgres;

internal static class PostgresQueueSql
{
    internal static string QuoteQualifiedIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new InvalidOperationException("PostgreSQL identifier cannot be empty.");

        return string.Join('.', identifier.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(QuoteSimpleIdentifier));
    }

    internal static string QuoteSimpleIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new InvalidOperationException("PostgreSQL identifier cannot be empty.");

        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }

    internal static string GetNotificationChannel(string tableName)
        => $"nymbroker_{string.Concat(tableName.Select(ch => char.IsLetterOrDigit(ch) ? char.ToLowerInvariant(ch) : '_'))}_changed";

    internal static string CreateSchema(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        var safeName = tableName.Replace('.', '_');
        var statusCreatedIndex = QuoteSimpleIdentifier($"ix_{safeName}_status_created");
        var statusLockedIndex = QuoteSimpleIdentifier($"ix_{safeName}_status_locked_until");

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
            CREATE INDEX IF NOT EXISTS {statusCreatedIndex}
                ON {table}(status, created_at_utc, queue_id);
            CREATE INDEX IF NOT EXISTS {statusLockedIndex}
                ON {table}(status, locked_until_utc);
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
                (@messageId, @status, NOW(), 0, @payload){notify}
            """;
    }

    internal static string ClaimMessages(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return $"""
            WITH claimed AS (
                SELECT queue_id
                FROM {table}
                WHERE status = @pendingStatus
                   OR (status = @inProgressStatus AND locked_until_utc IS NOT NULL AND locked_until_utc <= NOW())
                ORDER BY created_at_utc, queue_id
                FOR UPDATE SKIP LOCKED
                LIMIT @batchSize
            )
            UPDATE {table} AS m
            SET status = @inProgressStatus,
                locked_until_utc = NOW() + (@leaseTimeout * INTERVAL '1 second'),
                attempt_count = m.attempt_count + 1,
                last_error = NULL,
                failed_at_utc = NULL
            FROM claimed
            WHERE m.queue_id = claimed.queue_id
            RETURNING m.queue_id, m.message_id, m.payload, m.attempt_count;
            """;
    }

    internal static string FinalizeCompleted(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return $"""
            UPDATE {table}
            SET status = @completedStatus,
                locked_until_utc = NULL,
                completed_at_utc = NOW(),
                failed_at_utc = NULL,
                last_error = NULL
            WHERE queue_id = ANY(@queueIds)
              AND status = @inProgressStatus
            """;
    }

    internal static string FinalizeWithErrors(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return $"""
            UPDATE {table} AS target
            SET status = @status,
                locked_until_utc = NULL,
                last_error = source.error,
                failed_at_utc = CASE WHEN @status = @failedStatus THEN NOW() ELSE NULL END,
                completed_at_utc = NULL
            FROM unnest(@queueIds, @errors) AS source(queue_id, error)
            WHERE target.queue_id = source.queue_id
              AND target.status = @inProgressStatus
            """;
    }

    internal static string Listen(string tableName)
        => $"LISTEN {QuoteSimpleIdentifier(GetNotificationChannel(tableName))};";
}
