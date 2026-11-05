using NymBroker.Core.Endpoint.Queue;
using NymBroker.Shared;

namespace NymBroker.Endpoint.SqlServer;

/// <summary>
/// T-SQL for the queue table. Status values are inlined as literals (0 Pending, 1 InProgress,
/// 2 Completed, 3 Failed) rather than passed as parameters: SQL Server only uses the filtered
/// "active" index when it can prove the predicate matches the index filter at compile time.
/// </summary>
internal static class SqlServerQueueSql
{
    internal const int Pending = (int)QueueMessageStatus.Pending;
    internal const int InProgress = (int)QueueMessageStatus.InProgress;
    internal const int Completed = (int)QueueMessageStatus.Completed;
    internal const int Failed = (int)QueueMessageStatus.Failed;

    internal static string QuoteQualifiedIdentifier(string identifier) => SqlServerIdentifier.QuoteQualified(identifier);

    internal static string QuoteSimpleIdentifier(string identifier) => SqlServerIdentifier.QuoteSimple(identifier);

    private static string Literal(string value) => SqlServerIdentifier.Literal(value);

    /// <summary>
    /// Clustered on the IDENTITY key (append-only inserts, FIFO order). One filtered index covers only
    /// Pending/InProgress rows, so claiming stays cheap no matter how many Completed/Failed rows accumulate; one more
    /// per finished status finds rows for the retention cleanup.
    /// </summary>
    internal static string CreateSchema(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        var tableLiteral = Literal(table);
        var safeName = tableName.Replace('.', '_');
        var primaryKey = $"pk_{safeName}";
        var activeIndex = $"ix_{safeName}_active";
        var completedIndex = $"ix_{safeName}_completed";
        var failedIndex = $"ix_{safeName}_failed";
        var deferred = DeferredTable(tableName);
        var dueIndex = $"ix_{safeName}_deferred_due";

        return $"""
            SET NOCOUNT ON;
            IF OBJECT_ID(N'{tableLiteral}', N'U') IS NULL
            BEGIN
                CREATE TABLE {table} (
                    queue_id         BIGINT IDENTITY(1,1) NOT NULL,
                    message_id       UNIQUEIDENTIFIER NOT NULL,
                    status           INT              NOT NULL DEFAULT {Pending} CHECK (status IN ({Pending}, {InProgress}, {Completed}, {Failed})),
                    created_at_utc   DATETIME2(7)     NOT NULL DEFAULT SYSUTCDATETIME(),
                    locked_until_utc DATETIME2(7)     NULL,
                    completed_at_utc DATETIME2(7)     NULL,
                    failed_at_utc    DATETIME2(7)     NULL,
                    attempt_count    INT              NOT NULL DEFAULT 0,
                    last_error       NVARCHAR(MAX)    NULL,
                    payload          VARBINARY(MAX)   NOT NULL,
                    CONSTRAINT {QuoteSimpleIdentifier(primaryKey)} PRIMARY KEY CLUSTERED (queue_id)
                );
            END;
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{Literal(activeIndex)}' AND object_id = OBJECT_ID(N'{tableLiteral}'))
                CREATE INDEX {QuoteSimpleIdentifier(activeIndex)}
                    ON {table}(queue_id) INCLUDE (status, locked_until_utc)
                    WHERE status IN ({Pending}, {InProgress});
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{Literal(completedIndex)}' AND object_id = OBJECT_ID(N'{tableLiteral}'))
                CREATE INDEX {QuoteSimpleIdentifier(completedIndex)}
                    ON {table}(completed_at_utc)
                    WHERE status = {Completed};
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{Literal(failedIndex)}' AND object_id = OBJECT_ID(N'{tableLiteral}'))
                CREATE INDEX {QuoteSimpleIdentifier(failedIndex)}
                    ON {table}(failed_at_utc)
                    WHERE status = {Failed};
            IF OBJECT_ID(N'{Literal(deferred)}', N'U') IS NULL
                CREATE TABLE {deferred} (
                    deferred_id    BIGINT IDENTITY(1,1) NOT NULL,
                    deliver_at_utc DATETIME2(7)     NOT NULL,
                    message_id     UNIQUEIDENTIFIER NOT NULL,
                    payload        VARBINARY(MAX)   NOT NULL,
                    CONSTRAINT {QuoteSimpleIdentifier($"pk_{safeName}_deferred")} PRIMARY KEY CLUSTERED (deferred_id)
                );
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{Literal(dueIndex)}' AND object_id = OBJECT_ID(N'{Literal(deferred)}'))
                CREATE INDEX {QuoteSimpleIdentifier(dueIndex)} ON {deferred}(deliver_at_utc);
            """;
    }

    internal static string DeleteCompleted(string tableName) => DeleteFinished(tableName, Completed, "completed_at_utc");

    internal static string DeleteFailed(string tableName) => DeleteFinished(tableName, Failed, "failed_at_utc");

    /// <summary>
    /// One batch of old finished rows, found through the status's filtered index. <c>READPAST</c> lets several instances do
    /// this at the same time. No <c>SET NOCOUNT ON</c>: the caller uses the row count.
    /// </summary>
    private static string DeleteFinished(string tableName, int status, string finishedAtColumn) => $"""
        DELETE TOP (@batchSize) FROM {QuoteQualifiedIdentifier(tableName)} WITH (UPDLOCK, READPAST, ROWLOCK)
        WHERE status = {status}
          AND {finishedAtColumn} < DATEADD(SECOND, -@olderThanSeconds, SYSUTCDATETIME());
        """;

    /// <summary>
    /// Deferred messages wait in their own table, not in the queue table, so any number of them leaves the claim's
    /// index scan untouched and older versions of this package never deliver them early.
    /// </summary>
    private static string DeferredTable(string tableName) => QuoteQualifiedIdentifier($"{tableName}_deferred");

    internal static string InsertDeferred(string tableName) => $"""
        SET NOCOUNT ON;
        INSERT INTO {DeferredTable(tableName)} (deliver_at_utc, message_id, payload) VALUES (@deliverAt, @messageId, @payload);
        """;

    /// <summary>
    /// Moves up to <c>@batchSize</c> due messages to the queue in one transaction and returns how many. Through a table
    /// variable, because <c>OUTPUT INTO</c> cannot write to a table with a CHECK constraint.
    /// </summary>
    internal static string PromoteDue(string tableName) => $"""
        SET NOCOUNT ON;
        SET XACT_ABORT ON;
        DECLARE @due TABLE (message_id UNIQUEIDENTIFIER NOT NULL, payload VARBINARY(MAX) NOT NULL);
        DECLARE @moved INT;
        BEGIN TRANSACTION;
        DELETE TOP (@batchSize) FROM {DeferredTable(tableName)} WITH (UPDLOCK, READPAST, ROWLOCK)
        OUTPUT deleted.message_id, deleted.payload INTO @due (message_id, payload)
        WHERE deliver_at_utc <= SYSUTCDATETIME();
        SET @moved = @@ROWCOUNT;
        IF @moved > 0
            INSERT INTO {QuoteQualifiedIdentifier(tableName)} (message_id, status, created_at_utc, attempt_count, payload)
            SELECT message_id, {Pending}, SYSUTCDATETIME(), 0, payload FROM @due;
        COMMIT TRANSACTION;
        SELECT @moved;
        """;

    internal static string InsertMessage(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return $"""
            SET NOCOUNT ON;
            INSERT INTO {table}
                (message_id, status, created_at_utc, attempt_count, payload)
            VALUES
                (@messageId, {Pending}, SYSUTCDATETIME(), 0, @payload);
            """;
    }

    /// <summary>
    /// Inserts a batch in one statement (atomic). <c>@items</c> is a JSON array of <c>{"i": index, "p": base64 payload}</c>;
    /// OPENJSON decodes base64 into VARBINARY. <c>ORDER BY i</c> makes IDENTITY values — and delivery order — follow the batch.
    /// </summary>
    internal static string InsertMessages(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return $"""
            SET NOCOUNT ON;
            INSERT INTO {table}
                (message_id, status, created_at_utc, attempt_count, payload)
            SELECT NEWID(), {Pending}, SYSUTCDATETIME(), 0, items.payload
            FROM OPENJSON(@items) WITH (i INT '$.i', payload VARBINARY(MAX) '$.p') AS items
            ORDER BY items.i;
            """;
    }

    /// <summary>
    /// One round trip, one commit: finalizes the previous batch (when <c>@items</c> is not NULL) and claims the
    /// next one. <c>@items</c> is a JSON array of <c>{"id", "attempt", "status", "error"}</c> objects.
    /// The finalize only touches rows still InProgress with the same attempt number, so a late finalize never
    /// overwrites a row another poller has re-claimed after its lease expired.
    /// <c>UPDLOCK, READPAST, ROWLOCK</c> is the SQL Server equivalent of <c>FOR UPDATE SKIP LOCKED</c>.
    /// </summary>
    internal static string FinalizeAndClaim(string tableName)
        => $"""
            SET NOCOUNT ON;
            SET XACT_ABORT ON;
            BEGIN TRANSACTION;
            {FinalizeStatement(tableName)}
            {ClaimStatement(tableName)}
            COMMIT TRANSACTION;
            """;

    /// <summary>Finalize only — used for the last batch when the listener stops.</summary>
    internal static string Finalize(string tableName)
        => $"""
            SET NOCOUNT ON;
            {FinalizeStatement(tableName)}
            """;

    private static string FinalizeStatement(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return $"""
            IF @items IS NOT NULL
                UPDATE target
                SET status           = source.status,
                    locked_until_utc = NULL,
                    completed_at_utc = CASE WHEN source.status = {Completed} THEN SYSUTCDATETIME() ELSE NULL END,
                    failed_at_utc    = CASE WHEN source.status = {Failed} THEN SYSUTCDATETIME() ELSE NULL END,
                    last_error       = source.error
                FROM OPENJSON(@items) WITH (
                        queue_id BIGINT        '$.id',
                        attempt  INT           '$.attempt',
                        status   INT           '$.status',
                        error    NVARCHAR(MAX) '$.error') AS source
                INNER LOOP JOIN {table} AS target WITH (FORCESEEK)
                  ON target.queue_id = source.queue_id
                WHERE target.status = {InProgress}
                  AND target.attempt_count = source.attempt;
            """;
    }

    private static string ClaimStatement(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return $"""
            WITH claimed AS (
                SELECT TOP (@batchSize) queue_id, message_id, payload, status, locked_until_utc, attempt_count, last_error, failed_at_utc
                FROM {table} WITH (UPDLOCK, READPAST, ROWLOCK)
                WHERE status IN ({Pending}, {InProgress})
                  AND (status = {Pending} OR locked_until_utc <= SYSUTCDATETIME())
                ORDER BY queue_id
            )
            UPDATE claimed
            SET status           = {InProgress},
                locked_until_utc = DATEADD(SECOND, @leaseTimeout, SYSUTCDATETIME()),
                attempt_count    = attempt_count + 1,
                last_error       = NULL,
                failed_at_utc    = NULL
            OUTPUT inserted.queue_id, inserted.message_id, inserted.payload, inserted.attempt_count;
            """;
    }

    /// <summary>One statement for all five counts.</summary>
    internal static string InspectCounts(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return $"""
            SELECT (SELECT COUNT_BIG(*) FROM {table} WHERE status = {Pending}),
                   (SELECT COUNT_BIG(*) FROM {table} WHERE status = {InProgress}),
                   (SELECT COUNT_BIG(*) FROM {table} WHERE status = {Failed}),
                   (SELECT COUNT_BIG(*) FROM {table} WHERE status = {Completed}),
                   (SELECT COUNT_BIG(*) FROM {DeferredTable(tableName)})
            """;
    }

    /// <summary>
    /// One keyset page. Columns: key1, key2, message_id, created_at_utc, deliver_at_utc, failed_at_utc,
    /// attempt_count, last_error, payload.
    /// </summary>
    internal static string InspectList(string tableName, QueueInspectionState state)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return state switch
        {
            QueueInspectionState.Pending or QueueInspectionState.InProgress => $"""
                SELECT TOP (@limit) queue_id, CAST(0 AS BIGINT), message_id, created_at_utc,
                       CAST(NULL AS DATETIME2(7)), CAST(NULL AS DATETIME2(7)), attempt_count, last_error, payload
                  FROM {table}
                 WHERE status = {(state == QueueInspectionState.Pending ? Pending : InProgress)}
                   AND queue_id > @after1
                 ORDER BY queue_id
                """,
            QueueInspectionState.Failed => $"""
                SELECT TOP (@limit) failed_at_utc AS key_time, queue_id AS key_id, message_id, created_at_utc,
                       CAST(NULL AS DATETIME2(7)), failed_at_utc, attempt_count, last_error, payload
                  FROM {table}
                 WHERE status = {Failed} AND failed_at_utc >= @since
                   AND (@hasCursor = 0 OR failed_at_utc > @afterTime
                        OR (failed_at_utc = @afterTime AND queue_id > @after2))
                 ORDER BY key_time, key_id
                """,
            _ => $"""
                SELECT TOP (@limit) deliver_at_utc AS key_time, deferred_id AS key_id, message_id, deliver_at_utc,
                       deliver_at_utc, CAST(NULL AS DATETIME2(7)), 0, CAST(NULL AS NVARCHAR(MAX)), payload
                  FROM {DeferredTable(tableName)}
                 WHERE @hasCursor = 0 OR deliver_at_utc > @afterTime
                       OR (deliver_at_utc = @afterTime AND deferred_id > @after2)
                 ORDER BY key_time, key_id
                """
        };
    }
}
