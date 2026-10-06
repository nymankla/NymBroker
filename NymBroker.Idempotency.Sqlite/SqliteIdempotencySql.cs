namespace NymBroker.Idempotency.Sqlite;

internal static class SqliteIdempotencySql
{
    internal const long InProgress = 1;
    internal const long Completed = 2;

    internal static string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new InvalidOperationException("SQLite identifier cannot be empty.");

        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }

    internal static string CreateSchema(string tableName)
    {
        var table = QuoteIdentifier(tableName);
        var index = QuoteIdentifier($"ix_{tableName}_expires");
        return $"""
            CREATE TABLE IF NOT EXISTS {table} (
                message_id     TEXT    PRIMARY KEY,
                status         INTEGER NOT NULL CHECK (status IN ({InProgress}, {Completed})),
                expires_at_utc INTEGER NOT NULL,
                created_at_utc INTEGER NOT NULL DEFAULT (unixepoch())
            );
            CREATE INDEX IF NOT EXISTS {index} ON {table}(expires_at_utc);
            """;
    }

    internal static string Claim(string tableName)
        => $"""
            INSERT INTO {QuoteIdentifier(tableName)} (message_id, status, expires_at_utc, created_at_utc)
            VALUES (@id, {InProgress}, unixepoch() + @leaseSeconds, unixepoch())
            ON CONFLICT(message_id) DO UPDATE
                SET status = {InProgress},
                    expires_at_utc = unixepoch() + @leaseSeconds
                WHERE expires_at_utc <= unixepoch()
            RETURNING message_id
            """;

    internal static string ExistingStatus(string tableName)
        => $"""
            SELECT status FROM {QuoteIdentifier(tableName)}
            WHERE message_id = @id AND expires_at_utc > unixepoch()
            """;

    internal static string Complete(string tableName)
        => $"""
            INSERT INTO {QuoteIdentifier(tableName)} (message_id, status, expires_at_utc, created_at_utc)
            VALUES (@id, {Completed}, unixepoch() + @ttlSeconds, unixepoch())
            ON CONFLICT(message_id) DO UPDATE
                SET status = {Completed},
                    expires_at_utc = unixepoch() + @ttlSeconds
            """;

    internal static string Release(string tableName)
        => $"""
            DELETE FROM {QuoteIdentifier(tableName)}
            WHERE message_id = @id AND status = {InProgress}
            """;

    internal static string DeleteExpired(string tableName)
        => $"""
            DELETE FROM {QuoteIdentifier(tableName)}
            WHERE message_id IN (
                SELECT message_id FROM {QuoteIdentifier(tableName)}
                WHERE expires_at_utc <= unixepoch()
                LIMIT @batchSize
            )
            """;
}
