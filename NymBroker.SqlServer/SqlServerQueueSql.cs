namespace NymBroker.SqlServer;

/// <summary>
/// T-SQL for the queue table. Status values are inlined as literals (0 Pending, 1 InProgress,
/// 2 Completed, 3 Failed) rather than passed as parameters: SQL Server only uses the filtered
/// "active" index when it can prove the predicate matches the index filter at compile time.
/// </summary>
internal static class SqlServerQueueSql
{
    internal const int Pending = 0;
    internal const int InProgress = 1;
    internal const int Completed = 2;
    internal const int Failed = 3;

    internal static string QuoteQualifiedIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new InvalidOperationException("SQL Server identifier cannot be empty.");

        return string.Join('.', identifier.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(QuoteSimpleIdentifier));
    }

    internal static string QuoteSimpleIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new InvalidOperationException("SQL Server identifier cannot be empty.");

        return $"[{identifier.Replace("]", "]]")}]";
    }

    /// <summary>Escapes a value for use inside an <c>N'...'</c> string literal.</summary>
    private static string Literal(string value) => value.Replace("'", "''");

    /// <summary>
    /// Clustered on the IDENTITY key (append-only inserts, FIFO order). A single filtered index covers only
    /// Pending/InProgress rows, so claiming stays cheap no matter how many Completed/Failed rows accumulate.
    /// </summary>
    internal static string CreateSchema(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        var tableLiteral = Literal(table);
        var safeName = tableName.Replace('.', '_');
        var primaryKey = $"pk_{safeName}";
        var activeIndex = $"ix_{safeName}_active";

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
            """;
    }

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
}
