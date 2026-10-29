namespace NymBroker.Scheduling.SqlServer;

/// <summary>T-SQL for the schedule table: one row per named scheduled action with the last claimed occurrence (UTC).</summary>
internal static class SqlServerScheduleSql
{
    internal static string QuoteQualifiedIdentifier(string identifier)
        => string.Join('.', identifier.Split('.', StringSplitOptions.TrimEntries).Select(QuoteSimpleIdentifier));

    internal static string QuoteSimpleIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new InvalidOperationException("SQL Server identifier cannot be empty.");
        return $"[{identifier.Replace("]", "]]")}]";
    }

    private static string Literal(string value) => value.Replace("'", "''");

    /// <summary>
    /// Idempotent, also when several instances start at once: the loser of a concurrent CREATE gets error 2714
    /// ("There is already an object named …"), which is ignored.
    /// </summary>
    internal static string CreateSchema(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        var primaryKey = $"pk_{string.Join('_', tableName.Split('.', StringSplitOptions.TrimEntries))}";

        return $"""
            SET NOCOUNT ON;
            IF OBJECT_ID(N'{Literal(table)}', N'U') IS NULL
            BEGIN TRY
                CREATE TABLE {table} (
                    name           NVARCHAR(200) NOT NULL,
                    last_run_utc   DATETIME2(7)  NOT NULL,
                    updated_at_utc DATETIME2(7)  NOT NULL DEFAULT SYSUTCDATETIME(),
                    CONSTRAINT {QuoteSimpleIdentifier(primaryKey)} PRIMARY KEY CLUSTERED (name)
                );
            END TRY
            BEGIN CATCH
                IF ERROR_NUMBER() <> 2714 THROW;
            END CATCH;
            """;
    }

    /// <summary>Parameter: <c>@name</c>. Returns the last run, or no row.</summary>
    internal static string GetLastRun(string tableName)
        => $"SELECT last_run_utc FROM {QuoteQualifiedIdentifier(tableName)} WHERE name = @name;";

    /// <summary>
    /// Parameters: <c>@name</c>, <c>@occurrence</c>. Returns 1 if this call recorded the occurrence, else 0.
    /// <list type="number">
    /// <item>Move an existing row forward if its run is earlier. <c>UPDLOCK</c> makes a concurrent claimer wait and
    /// then re-check the predicate, so only one of them wins.</item>
    /// <item>Otherwise insert. A primary-key violation means another instance inserted first: try the update once more,
    /// which succeeds only if that instance claimed an earlier occurrence.</item>
    /// </list>
    /// </summary>
    internal static string Claim(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return $"""
            SET NOCOUNT ON;

            UPDATE {table} WITH (UPDLOCK, ROWLOCK)
               SET last_run_utc = @occurrence, updated_at_utc = SYSUTCDATETIME()
             WHERE name = @name AND last_run_utc < @occurrence;

            IF @@ROWCOUNT > 0
            BEGIN
                SELECT 1;
                RETURN;
            END;

            IF EXISTS (SELECT 1 FROM {table} WHERE name = @name)
            BEGIN
                SELECT 0;
                RETURN;
            END;

            BEGIN TRY
                INSERT INTO {table} (name, last_run_utc, updated_at_utc)
                VALUES (@name, @occurrence, SYSUTCDATETIME());
                SELECT 1;
            END TRY
            BEGIN CATCH
                IF ERROR_NUMBER() NOT IN (2601, 2627) THROW;
                UPDATE {table} WITH (UPDLOCK, ROWLOCK)
                   SET last_run_utc = @occurrence, updated_at_utc = SYSUTCDATETIME()
                 WHERE name = @name AND last_run_utc < @occurrence;
                SELECT CASE WHEN @@ROWCOUNT > 0 THEN 1 ELSE 0 END;
            END CATCH;
            """;
    }
}
