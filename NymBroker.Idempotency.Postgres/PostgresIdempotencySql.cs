using System.Security.Cryptography;
using System.Text;

namespace NymBroker.Idempotency.Postgres;

internal static class PostgresIdempotencySql
{
    internal const short InProgress = 1;
    internal const short Completed = 2;

    internal static string QuoteQualifiedIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new InvalidOperationException("PostgreSQL identifier cannot be empty.");

        return string.Join('.', identifier.Split('.', StringSplitOptions.TrimEntries).Select(QuoteSimpleIdentifier));
    }

    internal static string QuoteSimpleIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new InvalidOperationException("PostgreSQL identifier cannot be empty.");

        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }

    internal static string CreateSchema(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        var parts = tableName.Split('.', StringSplitOptions.TrimEntries);
        var readableName = string.Concat(tableName.Select(ch => ch is >= 'a' and <= 'z' or >= 'A' and <= 'Z' or >= '0' and <= '9' or '_' ? ch : '_'));
        var suffix = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(tableName)))[..12].ToLowerInvariant();
        var indexName = $"ix_{readableName[..Math.Min(readableName.Length, 38)]}_{suffix}_expires";
        var qualifiedIndexName = parts.Length > 1
            ? $"{QuoteSimpleIdentifier(parts[0])}.{QuoteSimpleIdentifier(indexName)}"
            : QuoteSimpleIdentifier(indexName);

        return $"""
            CREATE TABLE IF NOT EXISTS {table} (
                message_id     UUID        PRIMARY KEY,
                status         SMALLINT    NOT NULL CHECK (status IN ({InProgress}, {Completed})),
                expires_at_utc TIMESTAMPTZ NOT NULL,
                created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS {qualifiedIndexName} ON {table}(expires_at_utc);
            """;
    }

    internal static string Claim(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return $"""
            INSERT INTO {table} AS target_row
                (message_id, status, expires_at_utc, created_at_utc)
            VALUES
                (@id, {InProgress}, NOW() + (@leaseSeconds * INTERVAL '1 second'), NOW())
            ON CONFLICT (message_id) DO UPDATE
                SET status = {InProgress},
                    expires_at_utc = NOW() + (@leaseSeconds * INTERVAL '1 second')
                WHERE target_row.expires_at_utc <= NOW()
            RETURNING message_id;
            """;
    }

    internal static string ExistingStatus(string tableName)
        => $"""
            SELECT status FROM {QuoteQualifiedIdentifier(tableName)}
            WHERE message_id = @id AND expires_at_utc > NOW()
            """;

    internal static string Complete(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return $"""
            INSERT INTO {table}
                (message_id, status, expires_at_utc, created_at_utc)
            VALUES
                (@id, {Completed}, NOW() + (@ttlSeconds * INTERVAL '1 second'), NOW())
            ON CONFLICT (message_id) DO UPDATE
                SET status = {Completed},
                    expires_at_utc = NOW() + (@ttlSeconds * INTERVAL '1 second')
            """;
    }

    internal static string Release(string tableName)
        => $"""
            DELETE FROM {QuoteQualifiedIdentifier(tableName)}
            WHERE message_id = @id AND status = {InProgress}
            """;

    internal static string DeleteExpired(string tableName)
        => $"""
            DELETE FROM {QuoteQualifiedIdentifier(tableName)}
            WHERE ctid IN (
                SELECT ctid FROM {QuoteQualifiedIdentifier(tableName)}
                WHERE expires_at_utc < NOW()
                LIMIT @batchSize
            )
            """;
}
