namespace NymBroker.Scheduling.Postgres;

/// <summary>SQL for the schedule table: one row per named scheduled action with the last claimed occurrence.</summary>
internal static class PostgresScheduleSql
{
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

    /// <summary>Parameter <c>lockKey</c>: serializes concurrent creators; run inside a transaction (see <c>PostgresScheduleStore.CreateSchemaAsync</c>).</summary>
    internal static string CreateSchema(string tableName) => $"""
        SELECT pg_advisory_xact_lock(hashtext(@lockKey));
        CREATE TABLE IF NOT EXISTS {QuoteQualifiedIdentifier(tableName)} (
            name           VARCHAR(200) PRIMARY KEY,
            last_run_utc   TIMESTAMPTZ  NOT NULL,
            updated_at_utc TIMESTAMPTZ  NOT NULL DEFAULT now()
        );
        """;

    /// <summary>Parameter: <c>name</c>. Returns the last run, or no row.</summary>
    internal static string GetLastRun(string tableName)
        => $"SELECT last_run_utc FROM {QuoteQualifiedIdentifier(tableName)} WHERE name = @name";

    /// <summary>
    /// Parameters: <c>name</c>, <c>occurrence</c>. Inserts the row, or moves it forward if the stored run is earlier;
    /// returns a row only if it wrote one (claimed). A concurrent claim of the same occurrence waits for the row lock,
    /// re-checks the WHERE against the committed row and writes nothing.
    /// </summary>
    internal static string Claim(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return $"""
            INSERT INTO {table} AS s (name, last_run_utc, updated_at_utc)
            VALUES (@name, @occurrence, now())
            ON CONFLICT (name) DO UPDATE
                SET last_run_utc = EXCLUDED.last_run_utc,
                    updated_at_utc = now()
                WHERE s.last_run_utc < EXCLUDED.last_run_utc
            RETURNING name
            """;
    }
}
