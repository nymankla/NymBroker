using NymBroker.Shared;

namespace NymBroker.Outbox.MySql;

/// <summary>
/// SQL for the outbox table; see <c>SqlServerOutboxSql</c> for the row states. Times except <c>deliver_at_utc</c> (the
/// caller's) are the server's <c>UTC_TIMESTAMP(6)</c>. Every <c>table</c> argument is already quoted by
/// <see cref="MySqlIdentifier.QuoteTableName"/>.
/// </summary>
internal static class MySqlOutboxSql
{
    private const int Pending = OutboxDialect.Pending;
    private const int Failed = OutboxDialect.Failed;

    internal static string CreateSchema(string table) => $"""
        CREATE TABLE IF NOT EXISTS {table} (
            id               BIGINT       NOT NULL AUTO_INCREMENT,
            endpoint_name    VARCHAR(400) NOT NULL,
            body             LONGBLOB     NOT NULL,
            deliver_at_utc   DATETIME(6)  NULL,
            status           TINYINT      NOT NULL,
            attempts         INT          NOT NULL DEFAULT 0,
            available_at_utc DATETIME(6)  NOT NULL,
            claim_id         BINARY(16)   NULL,
            last_error       TEXT         NULL,
            created_at_utc   DATETIME(6)  NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
            PRIMARY KEY (id),
            INDEX ix_due (status, available_at_utc, id),
            CHECK (status IN ({Pending}, {Failed}))
        ) ENGINE = InnoDB;
        """;

    /// <summary><see cref="OutboxStatements.Claim"/> is the first round trip of a claim, <see cref="LockDue"/>.</summary>
    internal static OutboxStatements Statements(string table)
    {
        const string ByClaim = "WHERE id = @id AND claim_id = @claimId";
        return new OutboxStatements(
            Insert: $"""
                INSERT INTO {table} (endpoint_name, body, deliver_at_utc, status, attempts, available_at_utc)
                VALUES (@endpoint, @body, @deliverAt, {Pending}, 0,
                        CASE WHEN @deliverAt > UTC_TIMESTAMP(6) THEN @deliverAt ELSE UTC_TIMESTAMP(6) END);
                """,
            Claim: LockDue(table),
            MarkSent: $"DELETE FROM {table} {ByClaim};",
            RetryLater: $"UPDATE {table} SET available_at_utc = UTC_TIMESTAMP(6) + INTERVAL (@delayMs * 1000) MICROSECOND, last_error = @error {ByClaim};",
            MarkFailed: $"UPDATE {table} SET status = {Failed}, last_error = @error {ByClaim};",
            Release: $"UPDATE {table} SET available_at_utc = UTC_TIMESTAMP(6), attempts = attempts - 1 {ByClaim};");
    }

    /// <summary>
    /// First round trip: opens a transaction and locks up to <c>@batchSize</c> due ids. READ COMMITTED takes no gap locks
    /// that would block the application's inserts, and <c>SKIP LOCKED</c> passes over rows another claim holds and rows of
    /// transactions still open. Followed by <see cref="Claim"/>, or a plain COMMIT when there are none.
    /// </summary>
    internal static string LockDue(string table) => $"""
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
        START TRANSACTION;
        SELECT id FROM {table}
         WHERE status = {Pending} AND available_at_utc <= UTC_TIMESTAMP(6)
         ORDER BY id LIMIT @batchSize
         FOR UPDATE SKIP LOCKED;
        """;

    /// <summary>Second round trip: claims the locked ids, returns their rows, commits. The ids are inlined (BIGINTs read from the table).</summary>
    internal static string Claim(string table, IReadOnlyList<long> ids)
    {
        var idList = string.Join(",", ids);
        return $"""
            UPDATE {table}
               SET claim_id = @claimId,
                   attempts = attempts + 1,
                   available_at_utc = UTC_TIMESTAMP(6) + INTERVAL @leaseSeconds SECOND
             WHERE id IN ({idList});
            SELECT id, endpoint_name, body, deliver_at_utc, attempts FROM {table} WHERE id IN ({idList}) ORDER BY id;
            COMMIT;
            """;
    }
}
