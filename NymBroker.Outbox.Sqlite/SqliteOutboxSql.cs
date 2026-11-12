using NymBroker.Shared;

namespace NymBroker.Outbox.Sqlite;

/// <summary>
/// SQL for the outbox table; see <c>SqlServerOutboxSql</c> for the row states. Times are INTEGER Unix milliseconds;
/// "now" is computed with <c>julianday</c>, which every SQLite version has, with millisecond precision.
/// </summary>
internal static class SqliteOutboxSql
{
    private const int Pending = OutboxDialect.Pending;
    private const int Failed = OutboxDialect.Failed;

    /// <summary>Now, in Unix milliseconds.</summary>
    internal const string NowMs = "CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)";

    internal static string QuoteIdentifier(string identifier) => SqliteStoreTable.Quote(identifier);

    internal static string CreateSchema(string tableName)
    {
        var table = QuoteIdentifier(tableName);
        return $"""
            CREATE TABLE IF NOT EXISTS {table} (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                endpoint_name    TEXT    NOT NULL,
                body             BLOB    NOT NULL,
                deliver_at_utc   INTEGER NULL,
                status           INTEGER NOT NULL CHECK (status IN ({Pending}, {Failed})),
                attempts         INTEGER NOT NULL DEFAULT 0,
                available_at_utc INTEGER NOT NULL,
                claim_id         TEXT    NULL,
                last_error       TEXT    NULL,
                created_at_utc   INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS {QuoteIdentifier($"ix_{tableName}_due")} ON {table}(status, available_at_utc, id);
            """;
    }

    internal static OutboxStatements Statements(string tableName)
    {
        var table = QuoteIdentifier(tableName);
        const string ByClaim = "WHERE id = @id AND claim_id = @claimId";
        return new OutboxStatements(
            Insert: $"""
                INSERT INTO {table} (endpoint_name, body, deliver_at_utc, status, attempts, available_at_utc, created_at_utc)
                VALUES (@endpoint, @body, @deliverAt, {Pending}, 0, MAX(COALESCE(@deliverAt, 0), {NowMs}), {NowMs})
                """,
            Claim: Claim(table),
            MarkSent: $"DELETE FROM {table} {ByClaim}",
            RetryLater: $"UPDATE {table} SET available_at_utc = {NowMs} + @delayMs, last_error = @error {ByClaim}",
            MarkFailed: $"UPDATE {table} SET status = {Failed}, last_error = @error {ByClaim}",
            Release: $"UPDATE {table} SET available_at_utc = {NowMs}, attempts = attempts - 1 {ByClaim}");
    }

    /// <summary>One writer at a time, and rows of a transaction still open are not visible to another connection.</summary>
    private static string Claim(string table) => $"""
        UPDATE {table}
           SET claim_id = @claimId,
               attempts = attempts + 1,
               available_at_utc = {NowMs} + @leaseSeconds * 1000
         WHERE id IN (
            SELECT id FROM {table}
             WHERE status = {Pending} AND available_at_utc <= {NowMs}
             ORDER BY id
             LIMIT @batchSize)
        RETURNING id, endpoint_name, body, deliver_at_utc, attempts
        """;
}
