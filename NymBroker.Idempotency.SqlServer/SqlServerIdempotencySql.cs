namespace NymBroker.Idempotency.SqlServer;

/// <summary>
/// T-SQL for the idempotency table. One row per message ID; <c>expires_at_utc</c> is the end of the lease while the
/// row is InProgress and the end of the TTL once it is Completed, so "live" always means <c>expires_at_utc &gt; now</c>
/// and cleanup is one range delete. Status values are inlined literals (1 InProgress, 2 Completed). All times are
/// the database server's <c>SYSUTCDATETIME()</c>, so application clocks don't matter.
/// </summary>
internal static class SqlServerIdempotencySql
{
    internal const int InProgress = 1;
    internal const int Completed = 2;

    // Values returned by Claim.
    internal const int ClaimedResult = 0;
    internal const int DuplicateResult = 1;
    internal const int InProgressResult = 2;

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
    /// Clustered on <c>message_id</c>: every operation is a single-row seek by ID, and rows are narrow, so the page
    /// splits of random-GUID inserts stay cheap. A second index on <c>expires_at_utc</c> serves the cleanup.
    /// </summary>
    internal static string CreateSchema(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        var tableLiteral = Literal(table);
        var safeName = string.Join('_', tableName.Split('.', StringSplitOptions.TrimEntries));
        var primaryKey = $"pk_{safeName}";
        var expiresIndex = $"ix_{safeName}_expires";

        return $"""
            SET NOCOUNT ON;
            IF OBJECT_ID(N'{tableLiteral}', N'U') IS NULL
            BEGIN
                CREATE TABLE {table} (
                    message_id     UNIQUEIDENTIFIER NOT NULL,
                    status         TINYINT          NOT NULL CHECK (status IN ({InProgress}, {Completed})),
                    expires_at_utc DATETIME2(7)     NOT NULL,
                    created_at_utc DATETIME2(7)     NOT NULL DEFAULT SYSUTCDATETIME(),
                    CONSTRAINT {QuoteSimpleIdentifier(primaryKey)} PRIMARY KEY CLUSTERED (message_id)
                );
            END;
            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{Literal(expiresIndex)}' AND object_id = OBJECT_ID(N'{tableLiteral}'))
                CREATE INDEX {QuoteSimpleIdentifier(expiresIndex)} ON {table}(expires_at_utc);
            """;
    }

    /// <summary>
    /// Atomic claim in one round trip. Parameters: <c>@id</c>, <c>@leaseSeconds</c>. Returns
    /// <see cref="ClaimedResult"/>, <see cref="DuplicateResult"/> or <see cref="InProgressResult"/>.
    /// <list type="number">
    /// <item>Take over an expired row (expired TTL or abandoned lease). <c>UPDLOCK</c> makes a concurrent taker wait and
    /// then re-check the predicate, so only one of them wins.</item>
    /// <item>Otherwise insert. A primary-key violation means another delivery got there first: report its row's state.</item>
    /// </list>
    /// </summary>
    internal static string Claim(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return $"""
            SET NOCOUNT ON;
            DECLARE @now DATETIME2(7) = SYSUTCDATETIME();

            UPDATE {table} WITH (UPDLOCK, ROWLOCK)
               SET status = {InProgress}, expires_at_utc = DATEADD(second, @leaseSeconds, @now)
             WHERE message_id = @id AND expires_at_utc <= @now;

            IF @@ROWCOUNT > 0
            BEGIN
                SELECT {ClaimedResult};
                RETURN;
            END;

            BEGIN TRY
                INSERT INTO {table} (message_id, status, expires_at_utc, created_at_utc)
                VALUES (@id, {InProgress}, DATEADD(second, @leaseSeconds, @now), @now);
                SELECT {ClaimedResult};
            END TRY
            BEGIN CATCH
                IF ERROR_NUMBER() NOT IN (2601, 2627) THROW;
                SELECT CASE WHEN status = {Completed} THEN {DuplicateResult} ELSE {InProgressResult} END
                  FROM {table} WHERE message_id = @id;
            END CATCH;
            """;
    }

    /// <summary>
    /// Marks the message processed for the TTL. Parameters: <c>@id</c>, <c>@ttlSeconds</c>. Inserts the row if it is
    /// gone (e.g. the lease expired and cleanup removed it), so the duplicate is still recorded.
    /// </summary>
    internal static string Complete(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        return $"""
            SET NOCOUNT ON;
            DECLARE @now DATETIME2(7) = SYSUTCDATETIME();

            UPDATE {table}
               SET status = {Completed}, expires_at_utc = DATEADD(second, @ttlSeconds, @now)
             WHERE message_id = @id;

            IF @@ROWCOUNT = 0
            BEGIN
                BEGIN TRY
                    INSERT INTO {table} (message_id, status, expires_at_utc, created_at_utc)
                    VALUES (@id, {Completed}, DATEADD(second, @ttlSeconds, @now), @now);
                END TRY
                BEGIN CATCH
                    IF ERROR_NUMBER() NOT IN (2601, 2627) THROW;
                    UPDATE {table}
                       SET status = {Completed}, expires_at_utc = DATEADD(second, @ttlSeconds, @now)
                     WHERE message_id = @id;
                END CATCH;
            END;
            """;
    }

    /// <summary>Forgets an open claim (never a completed row). Parameter: <c>@id</c>.</summary>
    internal static string Release(string tableName)
        => $"""
            SET NOCOUNT ON;
            DELETE FROM {QuoteQualifiedIdentifier(tableName)} WHERE message_id = @id AND status = {InProgress};
            """;

    /// <summary>Deletes up to <c>@batchSize</c> expired rows and returns how many it deleted.</summary>
    internal static string DeleteExpired(string tableName)
        => $"""
            SET NOCOUNT ON;
            DELETE TOP (@batchSize) FROM {QuoteQualifiedIdentifier(tableName)} WHERE expires_at_utc < SYSUTCDATETIME();
            SELECT @@ROWCOUNT;
            """;
}
