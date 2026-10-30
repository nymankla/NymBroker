namespace NymBroker.Scheduling.Sqlite;

/// <summary>
/// SQL for the schedule table: one row per named scheduled action with the last claimed occurrence, in Unix
/// milliseconds (occurrences are whole milliseconds).
/// </summary>
internal static class SqliteScheduleSql
{
    internal static string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new InvalidOperationException("SQLite identifier cannot be empty.");

        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }

    internal static string CreateSchema(string tableName) => $"""
        CREATE TABLE IF NOT EXISTS {QuoteIdentifier(tableName)} (
            name           TEXT    NOT NULL PRIMARY KEY COLLATE NOCASE,
            last_run_utc   INTEGER NOT NULL,
            updated_at_utc INTEGER NOT NULL DEFAULT (unixepoch())
        );
        """;

    /// <summary>Parameter: <c>@name</c>. Returns the last run in Unix milliseconds, or no row.</summary>
    internal static string GetLastRun(string tableName)
        => $"SELECT last_run_utc FROM {QuoteIdentifier(tableName)} WHERE name = @name";

    /// <summary>
    /// Parameters: <c>@name</c>, <c>@occurrence</c> (Unix milliseconds). Inserts the row, or moves it forward if the
    /// stored run is earlier; returns a row only if it wrote one (claimed).
    /// </summary>
    internal static string Claim(string tableName) => $"""
        INSERT INTO {QuoteIdentifier(tableName)} (name, last_run_utc, updated_at_utc)
        VALUES (@name, @occurrence, unixepoch())
        ON CONFLICT(name) DO UPDATE
            SET last_run_utc = excluded.last_run_utc,
                updated_at_utc = unixepoch()
            WHERE last_run_utc < excluded.last_run_utc
        RETURNING name
        """;
}
