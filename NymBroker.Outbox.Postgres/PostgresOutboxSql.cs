using System.Security.Cryptography;
using System.Text;
using NymBroker.Shared;

namespace NymBroker.Outbox.Postgres;

/// <summary>
/// SQL for the outbox table; see <c>SqlServerOutboxSql</c> for the row states. Times except <c>deliver_at_utc</c> (the
/// caller's) are the server's <c>clock_timestamp()</c>, not <c>NOW()</c>, which is fixed at the start of a transaction.
/// </summary>
internal static class PostgresOutboxSql
{
    private const int Pending = OutboxDialect.Pending;
    private const int Failed = OutboxDialect.Failed;

    internal static string QuoteQualifiedIdentifier(string identifier) => PostgresIdentifier.QuoteQualified(identifier);

    /// <summary>@lockKey: serializes concurrent creators (run inside a transaction by <c>PostgresStoreSchema.CreateAsync</c>).</summary>
    internal static string CreateSchema(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        var readableName = string.Concat(tableName.Select(ch => char.IsAsciiLetterOrDigit(ch) || ch == '_' ? ch : '_'));
        var suffix = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(tableName)))[..12].ToLowerInvariant();
        var indexName = $"ix_{readableName[..Math.Min(readableName.Length, 38)]}_{suffix}_due";

        return $"""
            SELECT pg_advisory_xact_lock(hashtext(@lockKey));
            CREATE TABLE IF NOT EXISTS {table} (
                id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                endpoint_name    TEXT        NOT NULL,
                body             BYTEA       NOT NULL,
                deliver_at_utc   TIMESTAMPTZ NULL,
                status           SMALLINT    NOT NULL CHECK (status IN ({Pending}, {Failed})),
                attempts         INTEGER     NOT NULL DEFAULT 0,
                available_at_utc TIMESTAMPTZ NOT NULL,
                claim_id         UUID        NULL,
                last_error       TEXT        NULL,
                created_at_utc   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
            );
            CREATE INDEX IF NOT EXISTS {PostgresIdentifier.QuoteSimple(indexName)} ON {table}(status, available_at_utc, id);
            """;
    }

    internal static OutboxStatements Statements(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        const string ByClaim = "WHERE id = @id AND claim_id = @claimId";
        return new OutboxStatements(
            Insert: $"""
                INSERT INTO {table} (endpoint_name, body, deliver_at_utc, status, attempts, available_at_utc)
                VALUES (@endpoint, @body, @deliverAt, {Pending}, 0, GREATEST(@deliverAt, clock_timestamp()))
                """,
            Claim: Claim(table),
            MarkSent: $"DELETE FROM {table} {ByClaim}",
            RetryLater: $"UPDATE {table} SET available_at_utc = clock_timestamp() + @delayMs * INTERVAL '1 millisecond', last_error = @error {ByClaim}",
            MarkFailed: $"UPDATE {table} SET status = {Failed}, last_error = @error {ByClaim}",
            Release: $"UPDATE {table} SET available_at_utc = clock_timestamp(), attempts = attempts - 1 {ByClaim}");
    }

    /// <summary>
    /// <c>SKIP LOCKED</c> passes over rows another claim holds; rows of transactions still open are not visible at all.
    /// (<c>GREATEST</c> in the insert ignores a null due time.)
    /// </summary>
    private static string Claim(string table) => $"""
        UPDATE {table}
           SET claim_id = @claimId,
               attempts = attempts + 1,
               available_at_utc = clock_timestamp() + @leaseSeconds * INTERVAL '1 second'
         WHERE id IN (
            SELECT id FROM {table}
             WHERE status = {Pending} AND available_at_utc <= clock_timestamp()
             ORDER BY id
             LIMIT @batchSize
             FOR UPDATE SKIP LOCKED)
        RETURNING id, endpoint_name, body, deliver_at_utc, attempts
        """;
}
