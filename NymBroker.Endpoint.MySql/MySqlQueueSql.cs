using System.Text.RegularExpressions;
using NymBroker.Core.Endpoint.Queue;

namespace NymBroker.Endpoint.MySql;

/// <summary>
/// SQL for the MySQL queue table. Status values are inlined as literals (0 Pending, 1 InProgress, 2 Completed,
/// 3 Failed) so the predicates match the <c>(status, queue_id)</c> index exactly. All times are the server's
/// <c>UTC_TIMESTAMP(6)</c>, so application clocks don't matter.
/// </summary>
internal static partial class MySqlQueueSql
{
    internal const int Pending = (int)QueueMessageStatus.Pending;
    internal const int InProgress = (int)QueueMessageStatus.InProgress;
    internal const int Completed = (int)QueueMessageStatus.Completed;
    internal const int Failed = (int)QueueMessageStatus.Failed;

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_$]*$")]
    private static partial Regex IdentifierPart();

    /// <summary>Validates and backtick-quotes <c>table</c> or <c>db.table</c>; throws <see cref="ArgumentException"/> otherwise.</summary>
    internal static string QuoteTableName(string tableName)
    {
        var parts = (tableName ?? "").Split('.');
        if (parts.Length > 2 || !parts.All(p => IdentifierPart().IsMatch(p)))
            throw new ArgumentException(
                $"TableName '{tableName}' is not valid: use 'table' or 'database.table' with letters, digits, '_' and '$'.", nameof(tableName));
        return string.Join('.', parts.Select(p => $"`{p}`"));
    }

    /// <summary>
    /// InnoDB, clustered on the AUTO_INCREMENT key (append-only inserts, FIFO order). MySQL has no partial index and a
    /// generated column cannot use the AUTO_INCREMENT key, so the claim index is <c>(status, queue_id)</c>: claiming
    /// reads only the <c>status = 0</c> range in id order, however many Completed/Failed rows accumulate.
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
            CHECK (status IN ({Pending}, {InProgress}, {Completed}, {Failed}))
        ) ENGINE = InnoDB;
        """;

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
