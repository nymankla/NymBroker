using System.Text.RegularExpressions;

namespace NymBroker.Scheduling.MySql;

/// <summary>SQL for the schedule table: one row per named scheduled action with the last claimed occurrence (UTC).</summary>
internal static partial class MySqlScheduleSql
{
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

    /// <summary><c>claim_token</c> identifies the winner of the latest claim.</summary>
    internal static string CreateSchema(string table) => $"""
        CREATE TABLE IF NOT EXISTS {table} (
            name           VARCHAR(200) NOT NULL,
            last_run_utc   DATETIME(6)  NOT NULL,
            claim_token    BINARY(16)   NULL,
            updated_at_utc DATETIME(6)  NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
            PRIMARY KEY (name)
        ) ENGINE = InnoDB;
        """;

    /// <summary>Parameter: <c>@name</c>. Returns the last run, or no row.</summary>
    internal static string GetLastRun(string table) => $"SELECT last_run_utc FROM {table} WHERE name = @name;";

    /// <summary>
    /// Parameters: <c>@name</c>, <c>@occurrence</c>, <c>@token</c> (a new random value per call). Inserts the row, or —
    /// under the row lock <c>ON DUPLICATE KEY UPDATE</c> takes — moves it forward if its run is earlier, stamping it with
    /// <c>@token</c>. The following SELECT returns whether the row carries our token: that, not the affected-rows count,
    /// decides the result, because the count depends on the connection's <c>UseAffectedRows</c> setting.
    /// <c>last_run_utc</c> is assigned last: MySQL evaluates the SET list left to right, and the other assignments test the old value.
    /// </summary>
    internal static string Claim(string table) => $"""
        INSERT INTO {table} (name, last_run_utc, claim_token, updated_at_utc)
        VALUES (@name, @occurrence, @token, UTC_TIMESTAMP(6))
        ON DUPLICATE KEY UPDATE
            claim_token    = IF(last_run_utc < @occurrence, @token, claim_token),
            updated_at_utc = IF(last_run_utc < @occurrence, UTC_TIMESTAMP(6), updated_at_utc),
            last_run_utc   = IF(last_run_utc < @occurrence, @occurrence, last_run_utc);
        SELECT claim_token <=> @token FROM {table} WHERE name = @name;
        """;
}
