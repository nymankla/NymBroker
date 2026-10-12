using System.Text.RegularExpressions;

namespace NymBroker.Idempotency.MySql;

/// <summary>
/// SQL for the idempotency table. One row per message ID; <c>expires_at_utc</c> is the end of the lease while the row
/// is InProgress and the end of the TTL once it is Completed, so "live" always means <c>expires_at_utc &gt; now</c> and
/// cleanup is one range delete. Status values are inlined literals (1 InProgress, 2 Completed). All times are the
/// server's <c>UTC_TIMESTAMP(6)</c>, which is constant within a statement.
/// </summary>
internal static partial class MySqlIdempotencySql
{
    internal const int InProgress = 1;
    internal const int Completed = 2;

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
    /// Clustered on <c>message_id</c> (every operation is a primary-key lookup); <c>claim_token</c> identifies the
    /// winner of a claim; a second index on <c>expires_at_utc</c> serves the cleanup.
    /// </summary>
    internal static string CreateSchema(string table) => $"""
        CREATE TABLE IF NOT EXISTS {table} (
            message_id     BINARY(16)  NOT NULL,
            status         TINYINT     NOT NULL,
            claim_token    BINARY(16)  NULL,
            expires_at_utc DATETIME(6) NOT NULL,
            created_at_utc DATETIME(6) NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
            PRIMARY KEY (message_id),
            INDEX ix_expires (expires_at_utc),
            CHECK (status IN ({InProgress}, {Completed}))
        ) ENGINE = InnoDB;
        """;

    /// <summary>
    /// Atomic claim in one round trip. Parameters: <c>@id</c>, <c>@token</c> (a new random value per call),
    /// <c>@leaseSeconds</c>. Inserts the row, or — under the row lock <c>ON DUPLICATE KEY UPDATE</c> takes — takes over a
    /// row whose lease or TTL has expired, stamping it with <c>@token</c>. The following SELECT returns the row's status
    /// and whether it carries our token: that, not the affected-rows count, decides the result, because the count
    /// depends on the connection's <c>UseAffectedRows</c> setting (CLIENT_FOUND_ROWS). <c>expires_at_utc</c> is
    /// assigned last: MySQL evaluates the SET list left to right, and the other assignments test the old value.
    /// </summary>
    internal static string Claim(string table) => $"""
        INSERT INTO {table} (message_id, status, claim_token, expires_at_utc, created_at_utc)
        VALUES (@id, {InProgress}, @token, UTC_TIMESTAMP(6) + INTERVAL @leaseSeconds SECOND, UTC_TIMESTAMP(6))
        ON DUPLICATE KEY UPDATE
            status         = IF(expires_at_utc <= UTC_TIMESTAMP(6), {InProgress}, status),
            claim_token    = IF(expires_at_utc <= UTC_TIMESTAMP(6), @token, claim_token),
            created_at_utc = IF(expires_at_utc <= UTC_TIMESTAMP(6), UTC_TIMESTAMP(6), created_at_utc),
            expires_at_utc = IF(expires_at_utc <= UTC_TIMESTAMP(6), UTC_TIMESTAMP(6) + INTERVAL @leaseSeconds SECOND, expires_at_utc);
        SELECT status, claim_token <=> @token FROM {table} WHERE message_id = @id;   -- null-safe: a row written by Complete has no token
        """;

    /// <summary>
    /// Marks the message processed for the TTL. Parameters: <c>@id</c>, <c>@ttlSeconds</c>. An upsert, so the duplicate
    /// is recorded even if the row is gone (the lease expired and cleanup removed it).
    /// </summary>
    internal static string Complete(string table) => $"""
        INSERT INTO {table} (message_id, status, expires_at_utc, created_at_utc)
        VALUES (@id, {Completed}, UTC_TIMESTAMP(6) + INTERVAL @ttlSeconds SECOND, UTC_TIMESTAMP(6))
        ON DUPLICATE KEY UPDATE
            status         = {Completed},
            expires_at_utc = UTC_TIMESTAMP(6) + INTERVAL @ttlSeconds SECOND;
        """;

    /// <summary>Forgets an open claim (never a completed row). Parameter: <c>@id</c>.</summary>
    internal static string Release(string table) => $"DELETE FROM {table} WHERE message_id = @id AND status = {InProgress};";

    /// <summary>Deletes up to <c>@batchSize</c> expired rows (oldest first, using the index).</summary>
    internal static string DeleteExpired(string table)
        => $"DELETE FROM {table} WHERE expires_at_utc < UTC_TIMESTAMP(6) ORDER BY expires_at_utc LIMIT @batchSize;";
}
