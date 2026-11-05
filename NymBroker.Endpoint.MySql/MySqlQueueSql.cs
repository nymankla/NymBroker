using NymBroker.Core.Endpoint.Queue;
using NymBroker.Shared;

namespace NymBroker.Endpoint.MySql;

/// <summary>
/// SQL for the MySQL queue table. Status values are inlined as literals (0 Pending, 1 InProgress, 2 Completed,
/// 3 Failed) so the predicates match the <c>(status, queue_id)</c> index exactly. All times are the server's
/// <c>UTC_TIMESTAMP(6)</c>, so application clocks don't matter.
/// </summary>
internal static class MySqlQueueSql
{
    internal const int Pending = (int)QueueMessageStatus.Pending;
    internal const int InProgress = (int)QueueMessageStatus.InProgress;
    internal const int Completed = (int)QueueMessageStatus.Completed;
    internal const int Failed = (int)QueueMessageStatus.Failed;

    private const string CompletedAtIndexName = "ix_completed_at";

    /// <inheritdoc cref="MySqlIdentifier.QuoteTableName"/>
    internal static string QuoteTableName(string tableName) => MySqlIdentifier.QuoteTableName(tableName);

    /// <summary>
    /// InnoDB, clustered on the AUTO_INCREMENT key (append-only inserts, FIFO order). MySQL has no partial index and a
    /// generated column cannot use the AUTO_INCREMENT key, so the claim index is <c>(status, queue_id)</c>: claiming
    /// reads only the <c>status = 0</c> range in id order, however many Completed/Failed rows accumulate. The retention
    /// cleanup finds Completed rows through <see cref="CompletedAtIndexName"/>, which leaves out <c>status</c> so that claiming
    /// a message doesn't write to it. Failed rows are few, so the <c>status = 3</c> range of the claim index is enough.
    /// </summary>
    internal static string CreateSchema(string table) => $"""
        CREATE TABLE IF NOT EXISTS {table} (
            queue_id         BIGINT      NOT NULL AUTO_INCREMENT,
            message_id       BINARY(16)  NOT NULL,
            status           TINYINT     NOT NULL DEFAULT {Pending},
            created_at_utc   DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
            locked_until_utc DATETIME(6) NULL,
            completed_at_utc DATETIME(6) NULL,
            failed_at_utc    DATETIME(6) NULL,
            attempt_count    INT         NOT NULL DEFAULT 0,
            last_error       MEDIUMTEXT  NULL,
            payload          LONGBLOB    NOT NULL,
            PRIMARY KEY (queue_id),
            INDEX ix_status_queue (status, queue_id),
            INDEX {CompletedAtIndexName} (completed_at_utc),
            CHECK (status IN ({Pending}, {InProgress}, {Completed}, {Failed}))
        ) ENGINE = InnoDB;
        CREATE TABLE IF NOT EXISTS {DeferredTable(table)} (
            deferred_id    BIGINT      NOT NULL AUTO_INCREMENT,
            deliver_at_utc DATETIME(6) NOT NULL,
            message_id     BINARY(16)  NOT NULL,
            payload        LONGBLOB    NOT NULL,
            PRIMARY KEY (deferred_id),
            INDEX ix_due (deliver_at_utc, deferred_id)
        ) ENGINE = InnoDB;
        """;

    /// <summary>Finds the index on a table from an older version; MySQL has no <c>CREATE INDEX IF NOT EXISTS</c>.</summary>
    internal static string FindCompletedAtIndex(string table) => $"SHOW INDEX FROM {table} WHERE Key_name = '{CompletedAtIndexName}';";

    internal static string AddCompletedAtIndex(string table) => $"ALTER TABLE {table} ADD INDEX {CompletedAtIndexName} (completed_at_utc);";

    internal static string DeleteCompleted(string table) => DeleteFinished(table, Completed, "completed_at_utc");

    internal static string DeleteFailed(string table) => DeleteFinished(table, Failed, "failed_at_utc");

    /// <summary>One statement provides a consistent snapshot of counts for every state.</summary>
    internal static string InspectCounts(string table) => $"""
        SELECT (SELECT COUNT(*) FROM {table} WHERE status = {Pending}),
               (SELECT COUNT(*) FROM {table} WHERE status = {InProgress}),
               (SELECT COUNT(*) FROM {table} WHERE status = {Failed}),
               (SELECT COUNT(*) FROM {table} WHERE status = {Completed}),
               (SELECT COUNT(*) FROM {DeferredTable(table)})
        """;

    /// <summary>One page after its keyset cursor; completed messages are count-only.</summary>
    internal static string InspectList(string table, QueueInspectionState state) => state switch
    {
        QueueInspectionState.Pending or QueueInspectionState.InProgress => $"""
            SELECT queue_id, 0, message_id, created_at_utc, NULL, NULL, attempt_count, last_error, payload
              FROM {table}
             WHERE status = {(state == QueueInspectionState.Pending ? Pending : InProgress)} AND queue_id > @after1
             ORDER BY queue_id
             LIMIT @limit
            """,
        QueueInspectionState.Failed => $"""
            SELECT failed_at_utc, queue_id, message_id, created_at_utc, NULL, failed_at_utc, attempt_count, last_error, payload
              FROM {table}
             WHERE status = {Failed} AND failed_at_utc >= @since
               AND (failed_at_utc > @afterTime OR (failed_at_utc = @afterTime AND queue_id > @after2))
             ORDER BY failed_at_utc, queue_id
             LIMIT @limit
            """,
        _ => $"""
            SELECT deliver_at_utc, deferred_id, message_id, deliver_at_utc, deliver_at_utc, NULL, 0, NULL, payload
              FROM {DeferredTable(table)}
             WHERE deliver_at_utc > @afterTime OR (deliver_at_utc = @afterTime AND deferred_id > @after2)
             ORDER BY deliver_at_utc, deferred_id
             LIMIT @limit
            """
    };

    /// <summary>One batch of old finished rows. READ COMMITTED, as for a claim, so the delete takes no gap locks that would block concurrent inserts.</summary>
    private static string DeleteFinished(string table, int status, string finishedAtColumn) => $"""
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
        DELETE FROM {table}
         WHERE status = {status}
           AND {finishedAtColumn} < UTC_TIMESTAMP(6) - INTERVAL @olderThanSeconds SECOND
         LIMIT @batchSize;
        """;

    /// <summary>
    /// Deferred messages wait in their own table, not in the queue table, so any number of them leaves the claim's index
    /// range untouched and older versions of this package never deliver them early. <paramref name="table"/> is quoted, so
    /// the suffix goes inside the closing backtick.
    /// </summary>
    private static string DeferredTable(string table) => $"{table[..^1]}_deferred`";

    internal static string InsertDeferred(string table)
        => $"INSERT INTO {DeferredTable(table)} (deliver_at_utc, message_id, payload) VALUES (@deliverAt, @messageId, @payload);";

    /// <summary>
    /// Opens a transaction and locks up to <c>@batchSize</c> due deferred ids. READ COMMITTED and <c>SKIP LOCKED</c>, as for
    /// a claim. Followed by <see cref="PromoteDue"/>, or by a plain COMMIT when there are none.
    /// </summary>
    internal static string LockDue(string table) => $"""
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
        START TRANSACTION;
        SELECT deferred_id FROM {DeferredTable(table)}
         WHERE deliver_at_utc <= UTC_TIMESTAMP(6)
         ORDER BY deliver_at_utc, deferred_id LIMIT @batchSize
         FOR UPDATE SKIP LOCKED;
        """;

    /// <summary>
    /// Moves the messages locked by <see cref="LockDue"/> to the queue and commits. The ids are inlined (they are BIGINTs
    /// read from the table).
    /// </summary>
    internal static string PromoteDue(string table, IReadOnlyList<long> deferredIds)
    {
        var idList = string.Join(",", deferredIds);
        return $"""
            INSERT INTO {table} (message_id, payload)
            SELECT message_id, payload FROM {DeferredTable(table)} WHERE deferred_id IN ({idList}) ORDER BY deliver_at_utc, deferred_id;
            DELETE FROM {DeferredTable(table)} WHERE deferred_id IN ({idList});
            COMMIT;
            """;
    }

    internal static string InsertMessage(string table) => $"INSERT INTO {table} (message_id, payload) VALUES (@messageId, @payload);";

    /// <summary>One multi-row INSERT for a batch (atomic); AUTO_INCREMENT values follow the VALUES order.</summary>
    internal static string InsertMessages(string table, int count)
        => $"INSERT INTO {table} (message_id, payload) VALUES {string.Join(", ", Enumerable.Range(0, count).Select(i => $"(@m{i}, @p{i})"))};";

    /// <summary>
    /// First round trip of a claim, opening the transaction: write the previous batch's outcomes (<c>@items</c>, a JSON
    /// array or NULL), return rows whose lease expired to Pending, then lock the next ids. READ COMMITTED, so the locking
    /// read takes no gap locks that would block concurrent inserts. The transaction is committed by <see cref="Claim"/>.
    /// </summary>
    internal static string FinalizeAndLock(string table) => $"""
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
        START TRANSACTION;
        {FinalizeStatement(table)}
        UPDATE {table} SET status = {Pending}, locked_until_utc = NULL
         WHERE status = {InProgress} AND locked_until_utc <= UTC_TIMESTAMP(6)
         ORDER BY queue_id LIMIT 1000;
        SELECT queue_id FROM {table}
         WHERE status = {Pending}
         ORDER BY queue_id LIMIT @batchSize
         FOR UPDATE SKIP LOCKED;
        """;

    /// <summary>Second round trip: claim the locked ids, return their rows, commit. The ids are inlined (they are BIGINTs read from the table).</summary>
    internal static string Claim(string table, IReadOnlyList<long> ids)
    {
        var idList = string.Join(",", ids);
        return $"""
            UPDATE {table}
               SET status           = {InProgress},
                   locked_until_utc = UTC_TIMESTAMP(6) + INTERVAL @leaseTimeout SECOND,
                   attempt_count    = attempt_count + 1,
                   last_error       = NULL,
                   failed_at_utc    = NULL
             WHERE queue_id IN ({idList});
            SELECT queue_id, message_id, payload, attempt_count FROM {table} WHERE queue_id IN ({idList}) ORDER BY queue_id;
            COMMIT;
            """;
    }

    /// <summary>Finalize only — used for the last batch when the listener stops (autocommit).</summary>
    internal static string Finalize(string table) => FinalizeStatement(table);

    /// <summary>
    /// Writes mixed outcomes in one UPDATE joined to <c>JSON_TABLE(@items)</c> (<c>{"id","attempt","status","error"}</c>);
    /// each row is found by primary key. The guard <c>status = InProgress AND attempt_count = attempt</c> makes a late
    /// write from a poller whose lease expired a no-op.
    /// </summary>
    private static string FinalizeStatement(string table) => $"""
        UPDATE {table} AS target
          JOIN JSON_TABLE(@items, '$[*]' COLUMNS (
                   id      BIGINT     PATH '$.id',
                   attempt INT        PATH '$.attempt',
                   status  INT        PATH '$.status',
                   error   MEDIUMTEXT PATH '$.error' NULL ON EMPTY)) AS source
            ON target.queue_id = source.id
           SET target.status           = source.status,
               target.locked_until_utc = NULL,
               target.completed_at_utc = IF(source.status = {Completed}, UTC_TIMESTAMP(6), NULL),
               target.failed_at_utc    = IF(source.status = {Failed}, UTC_TIMESTAMP(6), NULL),
               target.last_error       = source.error
         WHERE target.status = {InProgress}
           AND target.attempt_count = source.attempt;
        """;
}
