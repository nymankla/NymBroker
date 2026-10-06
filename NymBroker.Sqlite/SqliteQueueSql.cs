using System.Text.RegularExpressions;

namespace NymBroker.Sql;

/// <summary>
/// SQL for the queue table (schema version 2). Status values are inlined as literals (0 Pending, 1 InProgress,
/// 2 Completed, 3 Failed), never parameters: SQLite only uses the partial "active" index when it can prove the
/// query's predicate implies the index's <c>WHERE Status IN (0, 1)</c> when the statement is prepared.
/// </summary>
internal static partial class SqliteQueueSql
{
    internal const int Pending = 0;
    internal const int InProgress = 1;
    internal const int Completed = 2;
    internal const int Failed = 3;

    internal const int SchemaVersion = 2;

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]*$")]
    private static partial Regex TableNamePattern();

    internal static void ValidateTableName(string tableName)
    {
        if (string.IsNullOrEmpty(tableName) || !TableNamePattern().IsMatch(tableName))
            throw new ArgumentException(
                $"SQLite table name '{tableName}' is invalid: use letters, digits and underscores, not starting with a digit.",
                nameof(tableName));
    }

    /// <summary>Quotes a validated identifier (<c>"name"</c>, embedded quotes doubled).</summary>
    internal static string Quote(string identifier) => $"\"{identifier.Replace("\"", "\"\"")}\"";

    internal static string ActiveIndexName(string tableName) => $"IX_{tableName}_Active";

    /// <summary>
    /// No AUTOINCREMENT (it writes <c>sqlite_sequence</c> on every insert); <c>QueueId</c> is the rowid and still
    /// increases monotonically while rows exist. <c>MessageId</c> is a 16-byte GUID, only logged, so it has no index.
    /// <c>Payload</c> holds the raw bytes unchanged.
    /// </summary>
    internal static string CreateTable(string tableName) => $"""
        CREATE TABLE IF NOT EXISTS {Quote(tableName)} (
            QueueId        INTEGER NOT NULL PRIMARY KEY,
            MessageId      BLOB    NOT NULL,
            Status         INTEGER NOT NULL DEFAULT {Pending} CHECK (Status IN ({Pending}, {InProgress}, {Completed}, {Failed})),
            CreatedAtUtc   INTEGER NOT NULL DEFAULT (unixepoch()),
            LockedUntilUtc INTEGER NULL,
            CompletedAtUtc INTEGER NULL,
            FailedAtUtc    INTEGER NULL,
            AttemptCount   INTEGER NOT NULL DEFAULT 0,
            LastError      TEXT    NULL,
            Payload        BLOB    NOT NULL
        );
        CREATE INDEX IF NOT EXISTS {Quote(ActiveIndexName(tableName))} ON {Quote(tableName)}(QueueId) WHERE Status IN ({Pending}, {InProgress});
        PRAGMA user_version = {SchemaVersion};
        """;

    /// <summary>
    /// Copies a version-1 table (QueueId, TEXT MessageId UNIQUE, AUTOINCREMENT, TEXT Payload) into the new table.
    /// TEXT values cast to BLOB keep their UTF-8 bytes.
    /// </summary>
    internal static string CopyFromVersion1(string tableName, string backupTableName) => $"""
        INSERT INTO {Quote(tableName)}
            (QueueId, MessageId, Status, CreatedAtUtc, LockedUntilUtc, CompletedAtUtc, FailedAtUtc, AttemptCount, LastError, Payload)
        SELECT QueueId, CAST(MessageId AS BLOB), Status, CreatedAtUtc, LockedUntilUtc, CompletedAtUtc, FailedAtUtc, AttemptCount, LastError,
               CAST(Payload AS BLOB)
        FROM {Quote(backupTableName)};
        """;

    /// <summary>Copies the legacy single-status table (Id, Status 'Processed'/…, CreatedAt, ProcessedAt, Payload).</summary>
    internal static string CopyFromLegacy(string tableName, string backupTableName) => $"""
        INSERT INTO {Quote(tableName)}
            (MessageId, Status, CreatedAtUtc, LockedUntilUtc, CompletedAtUtc, FailedAtUtc, AttemptCount, LastError, Payload)
        SELECT CAST(Id AS BLOB),
               CASE WHEN Status = 'Processed' THEN {Completed} ELSE {Pending} END,
               COALESCE(unixepoch(CreatedAt), unixepoch()),
               NULL,
               CASE WHEN ProcessedAt IS NOT NULL THEN unixepoch(ProcessedAt) ELSE NULL END,
               NULL,
               CASE WHEN Status = 'Processed' THEN 1 ELSE 0 END,
               NULL,
               CAST(Payload AS BLOB)
        FROM {Quote(backupTableName)};
        """;

    /// <summary>The version-1 index names; after a rename they belong to the backup table, which doesn't need them.</summary>
    internal static string DropVersion1Indexes(string tableName) => $"""
        DROP INDEX IF EXISTS {Quote($"IX_{tableName}_Status_CreatedAt")};
        DROP INDEX IF EXISTS {Quote($"IX_{tableName}_LockedUntil")};
        """;

    internal static string Insert(string tableName) =>
        $"INSERT INTO {Quote(tableName)} (MessageId, Status, CreatedAtUtc, AttemptCount, Payload) VALUES ($messageId, {Pending}, unixepoch(), 0, $payload)";

    /// <summary>Claims up to <c>$batchSize</c> Pending or lease-expired rows in one statement (SQLite ≥ 3.35 for RETURNING).</summary>
    internal static string Claim(string tableName)
    {
        var t = Quote(tableName);
        return $"""
            UPDATE {t}
               SET Status = {InProgress},
                   LockedUntilUtc = unixepoch() + $leaseSeconds,
                   AttemptCount = AttemptCount + 1,
                   LastError = NULL,
                   FailedAtUtc = NULL
             WHERE QueueId IN (
                   SELECT QueueId FROM {t}
                    WHERE Status IN ({Pending}, {InProgress})   -- the partial index's own predicate, verbatim
                      AND (Status = {Pending} OR LockedUntilUtc <= unixepoch())
                    ORDER BY QueueId
                    LIMIT $batchSize)
            RETURNING QueueId, MessageId, Payload, AttemptCount
            """;
    }

    // Finalize updates. The guard (still InProgress with the attempt we claimed) stops a poller whose lease expired
    // from overwriting a row another poller has claimed since.

    internal static string MarkCompleted(string tableName) => $"""
        UPDATE {Quote(tableName)}
           SET Status = {Completed}, LockedUntilUtc = NULL, CompletedAtUtc = unixepoch(), FailedAtUtc = NULL, LastError = NULL
         WHERE QueueId = $id AND Status = {InProgress} AND AttemptCount = $attempt
        """;

    internal static string MarkFailed(string tableName) => $"""
        UPDATE {Quote(tableName)}
           SET Status = {Failed}, LockedUntilUtc = NULL, FailedAtUtc = unixepoch(), CompletedAtUtc = NULL, LastError = $error
         WHERE QueueId = $id AND Status = {InProgress} AND AttemptCount = $attempt
        """;

    internal static string MarkPending(string tableName) => $"""
        UPDATE {Quote(tableName)}
           SET Status = {Pending}, LockedUntilUtc = NULL, LastError = $error, FailedAtUtc = NULL, CompletedAtUtc = NULL
         WHERE QueueId = $id AND Status = {InProgress} AND AttemptCount = $attempt
        """;
}
