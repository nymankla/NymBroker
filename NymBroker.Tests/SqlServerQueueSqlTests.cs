using NymBroker.SqlServer;

namespace NymBroker.Tests;

public sealed class SqlServerQueueSqlTests
{
    [Fact]
    public void SqlServerSettings_Defaults_AreExpected()
    {
        var settings = new SqlServerSettings();

        Assert.Equal("dbo.nymbroker_messages", settings.TableName);
        Assert.Equal(10, settings.BatchSize);
        Assert.True(settings.AutoCreateTable);
        Assert.Equal(5, settings.MaxRetryCount);
        Assert.Equal(TimeSpan.FromMinutes(5), settings.LeaseTimeout);
        Assert.Contains("Database=nymbroker", settings.ConnectionString);
    }

    [Fact]
    public void QuoteQualifiedIdentifier_BracketsSchemaAndTable()
    {
        Assert.Equal("[dbo].[orders]", SqlServerQueueSql.QuoteQualifiedIdentifier("dbo.orders"));
        Assert.Equal("[orders]", SqlServerQueueSql.QuoteQualifiedIdentifier("orders"));
    }

    [Fact]
    public void QuoteSimpleIdentifier_EscapesClosingBracket()
    {
        Assert.Equal("[odd]]name]", SqlServerQueueSql.QuoteSimpleIdentifier("odd]name"));
    }

    [Fact]
    public void QuoteQualifiedIdentifier_ThrowsForEmptyIdentifier()
    {
        Assert.Throws<InvalidOperationException>(() => SqlServerQueueSql.QuoteQualifiedIdentifier(" "));
    }

    [Fact]
    public void CreateSchema_IsIdempotent_ClusteredOnQueueId_WithFilteredActiveIndex()
    {
        var sql = SqlServerQueueSql.CreateSchema("dbo.orders");

        Assert.Contains("IF OBJECT_ID(N'[dbo].[orders]', N'U') IS NULL", sql);
        Assert.Contains("CREATE TABLE [dbo].[orders]", sql);
        Assert.Contains("payload          VARBINARY(MAX)   NOT NULL", sql);
        Assert.Contains("CONSTRAINT [pk_dbo_orders] PRIMARY KEY CLUSTERED (queue_id)", sql);
        Assert.Contains("CREATE INDEX [ix_dbo_orders_active]", sql);
        Assert.Contains("WHERE status IN (0, 1)", sql);
        Assert.Contains("WHERE name = N'ix_dbo_orders_active'", sql);
        // message_id is per-insert and never looked up, so it carries no (random GUID) unique index.
        Assert.Contains("message_id       UNIQUEIDENTIFIER NOT NULL,", sql);
        Assert.DoesNotContain("NOT NULL UNIQUE", sql);
    }

    [Fact]
    public void CreateSchema_EscapesQuotesInObjectIdLiteral()
    {
        var sql = SqlServerQueueSql.CreateSchema("o'brien");

        Assert.Contains("OBJECT_ID(N'[o''brien]', N'U')", sql);
    }

    [Fact]
    public void InsertMessage_UsesParameters_AndPendingLiteral()
    {
        var sql = SqlServerQueueSql.InsertMessage("orders");

        Assert.Contains("INSERT INTO [orders]", sql);
        Assert.Contains("(@messageId, 0, SYSUTCDATETIME(), 0, @payload)", sql);
    }

    [Fact]
    public void FinalizeAndClaim_IsOneTransactionalBatch()
    {
        var sql = SqlServerQueueSql.FinalizeAndClaim("orders");

        Assert.Contains("SET XACT_ABORT ON;", sql);
        Assert.Contains("BEGIN TRANSACTION;", sql);
        Assert.Contains("COMMIT TRANSACTION;", sql);
        Assert.True(sql.IndexOf("UPDATE target", StringComparison.Ordinal) < sql.IndexOf("WITH claimed AS", StringComparison.Ordinal),
            "the previous batch must be finalized before the next one is claimed");
    }

    [Fact]
    public void FinalizeAndClaim_ClaimUsesReadPast_LiteralStatuses_AndOutputsPayload()
    {
        var sql = SqlServerQueueSql.FinalizeAndClaim("orders");

        Assert.Contains("WITH (UPDLOCK, READPAST, ROWLOCK)", sql);
        Assert.Contains("SELECT TOP (@batchSize)", sql);
        // Literals (not parameters) so the optimizer can match the filtered index.
        Assert.Contains("WHERE status IN (0, 1)", sql);
        Assert.Contains("ORDER BY queue_id", sql);
        Assert.Contains("DATEADD(SECOND, @leaseTimeout, SYSUTCDATETIME())", sql);
        Assert.Contains("OUTPUT inserted.queue_id, inserted.message_id, inserted.payload, inserted.attempt_count", sql);
    }

    [Fact]
    public void Finalize_SeeksByQueueId_AndGuardsOnInProgressAndAttempt()
    {
        var sql = SqlServerQueueSql.Finalize("orders");

        Assert.Contains("IF @items IS NOT NULL", sql);
        Assert.Contains("FROM OPENJSON(@items) WITH (", sql);
        Assert.Contains("INNER LOOP JOIN [orders] AS target WITH (FORCESEEK)", sql);
        Assert.Contains("status   INT           '$.status'", sql);
        Assert.Contains("WHERE target.status = 1", sql);
        Assert.Contains("AND target.attempt_count = source.attempt", sql);
        Assert.Contains("CASE WHEN source.status = 2 THEN SYSUTCDATETIME() ELSE NULL END", sql);
        Assert.Contains("CASE WHEN source.status = 3 THEN SYSUTCDATETIME() ELSE NULL END", sql);
        Assert.DoesNotContain("WITH claimed AS", sql);
    }
}
