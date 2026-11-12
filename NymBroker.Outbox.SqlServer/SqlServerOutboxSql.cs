using NymBroker.Shared;

namespace NymBroker.Outbox.SqlServer;

/// <summary>
/// T-SQL for the outbox table. A row is Pending (1) until it is sent, when it is deleted, or Failed (2) after the last
/// attempt. <c>available_at_utc</c> is when a Pending row may next be claimed: its due time, then the end of a claim's
/// lease or a retry's delay. All times except <c>deliver_at_utc</c> (the caller's) are the server's <c>SYSUTCDATETIME()</c>.
/// </summary>
internal static class SqlServerOutboxSql
{
    private const int Pending = OutboxDialect.Pending;
    private const int Failed = OutboxDialect.Failed;

    internal static string QuoteQualifiedIdentifier(string identifier) => SqlServerIdentifier.QuoteQualified(identifier);

    /// <summary>
    /// Clustered on the identity <c>id</c> (insert order); a second index on (status, available_at_utc, id) finds due rows.
    /// A transaction-owned application lock serializes schema checks and DDL across store instances.
    /// </summary>
    internal static string CreateSchema(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        var tableLiteral = SqlServerIdentifier.Literal(table);
        var safeName = string.Join('_', tableName.Split('.', StringSplitOptions.TrimEntries));
        var dueIndex = $"ix_{safeName}_due";

        return $"""
            SET NOCOUNT ON;
            SET XACT_ABORT ON;
            BEGIN TRY
                BEGIN TRANSACTION;
                DECLARE @lockResult int;
                EXEC @lockResult = sys.sp_getapplock
                    @Resource = N'NymBroker:OutboxSchema',
                    @LockMode = N'Exclusive',
                    @LockOwner = N'Transaction';
                IF @lockResult < 0
                    THROW 51000, 'Could not acquire the outbox schema lock.', 1;

                IF OBJECT_ID(N'{tableLiteral}', N'U') IS NULL
                    CREATE TABLE {table} (
                        id               BIGINT IDENTITY(1, 1) NOT NULL,
                        endpoint_name    NVARCHAR(400)    NOT NULL,
                        body             VARBINARY(MAX)   NOT NULL,
                        deliver_at_utc   DATETIME2(7)     NULL,
                        status           TINYINT          NOT NULL CHECK (status IN ({Pending}, {Failed})),
                        attempts         INT              NOT NULL DEFAULT 0,
                        available_at_utc DATETIME2(7)     NOT NULL,
                        claim_id         UNIQUEIDENTIFIER NULL,
                        last_error       NVARCHAR(4000)   NULL,
                        created_at_utc   DATETIME2(7)     NOT NULL DEFAULT SYSUTCDATETIME(),
                        CONSTRAINT {SqlServerIdentifier.QuoteSimple($"pk_{safeName}")} PRIMARY KEY CLUSTERED (id)
                    );

                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{SqlServerIdentifier.Literal(dueIndex)}' AND object_id = OBJECT_ID(N'{tableLiteral}'))
                    CREATE INDEX {SqlServerIdentifier.QuoteSimple(dueIndex)} ON {table}(status, available_at_utc, id);

                COMMIT TRANSACTION;
            END TRY
            BEGIN CATCH
                IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
                THROW;
            END CATCH;
            """;
    }

    internal static OutboxStatements Statements(string tableName)
    {
        var table = QuoteQualifiedIdentifier(tableName);
        const string ByClaim = "WHERE id = @id AND claim_id = @claimId";
        return new OutboxStatements(
            Insert: $"""
                INSERT INTO {table} (endpoint_name, body, deliver_at_utc, status, attempts, available_at_utc)
                VALUES (@endpoint, @body, @deliverAt, {Pending}, 0,
                        CASE WHEN @deliverAt > SYSUTCDATETIME() THEN @deliverAt ELSE SYSUTCDATETIME() END);
                """,
            Claim: Claim(table),
            MarkSent: $"DELETE FROM {table} {ByClaim};",
            RetryLater: $"UPDATE {table} SET available_at_utc = DATEADD(millisecond, @delayMs, SYSUTCDATETIME()), last_error = @error {ByClaim};",
            MarkFailed: $"UPDATE {table} SET status = {Failed}, last_error = @error {ByClaim};",
            Release: $"UPDATE {table} SET available_at_utc = SYSUTCDATETIME(), attempts = attempts - 1 {ByClaim};");
    }

    /// <summary>
    /// <c>READPAST</c> skips rows locked by another claim and rows inserted by transactions still open, instead of waiting
    /// for them; <c>UPDLOCK</c> keeps two claims from taking the same row.
    /// </summary>
    private static string Claim(string table) => $"""
        SET NOCOUNT ON;
        WITH due AS (
            SELECT TOP (@batchSize) id, endpoint_name, body, deliver_at_utc, attempts, available_at_utc, claim_id
              FROM {table} WITH (UPDLOCK, READPAST, ROWLOCK)
             WHERE status = {Pending} AND available_at_utc <= SYSUTCDATETIME()
             ORDER BY id)
        UPDATE due
           SET claim_id = @claimId,
               attempts = attempts + 1,
               available_at_utc = DATEADD(second, @leaseSeconds, SYSUTCDATETIME())
        OUTPUT inserted.id, inserted.endpoint_name, inserted.body, inserted.deliver_at_utc, inserted.attempts;
        """;
}
