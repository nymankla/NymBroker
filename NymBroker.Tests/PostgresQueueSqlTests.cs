using NymBroker.Postgres;

namespace NymBroker.Tests;

public sealed class PostgresQueueSqlTests
{
    [Fact]
    public void PostgresSettings_Defaults_AreExpected()
    {
        var settings = new PostgresSettings();

        Assert.Equal("nymbroker_messages", settings.TableName);
        Assert.Equal(10, settings.BatchSize);
        Assert.True(settings.AutoCreateTable);
        Assert.True(settings.UseNotifications);
        Assert.Equal(5, settings.MaxRetryCount);
    }

    [Fact]
    public void QuoteQualifiedIdentifier_QuotesSchemaAndTable()
    {
        var result = PostgresQueueSql.QuoteQualifiedIdentifier("public.orders");

        Assert.Equal("\"public\".\"orders\"", result);
    }

    [Fact]
    public void QuoteQualifiedIdentifier_ThrowsForEmptyIdentifier()
    {
        Assert.Throws<InvalidOperationException>(() => PostgresQueueSql.QuoteQualifiedIdentifier(" "));
    }

    [Fact]
    public void GetNotificationChannel_NormalizesIdentifier()
    {
        var result = PostgresQueueSql.GetNotificationChannel("sales.orders-v1");

        Assert.Equal("nymbroker_sales_orders_v1_changed", result);
    }

    [Fact]
    public void CreateSchema_UsesByteaPayload_AndPartialActiveIndex()
    {
        var sql = PostgresQueueSql.CreateSchema("public.orders");

        Assert.Contains("payload          BYTEA       NOT NULL", sql);
        Assert.Contains("CREATE TABLE IF NOT EXISTS \"public\".\"orders\"", sql);
        Assert.Contains("CREATE INDEX IF NOT EXISTS \"ix_public_orders_active\"", sql);
        Assert.Contains("ON \"public\".\"orders\"(created_at_utc, queue_id)", sql);
        Assert.Contains("WHERE status IN (0, 1);", sql);
    }

    [Fact]
    public void CreateSchema_DropsLegacyStatusIndexes_InTheTablesSchema()
    {
        Assert.Contains("DROP INDEX IF EXISTS \"public\".\"ix_public_orders_status_created\";", PostgresQueueSql.CreateSchema("public.orders"));
        Assert.Contains("DROP INDEX IF EXISTS \"public\".\"ix_public_orders_status_locked_until\";", PostgresQueueSql.CreateSchema("public.orders"));
        Assert.Contains("DROP INDEX IF EXISTS \"ix_orders_status_created\";", PostgresQueueSql.CreateSchema("orders"));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void InsertMessage_OptionallyAddsNotify(bool notifyListeners)
    {
        var sql = PostgresQueueSql.InsertMessage("orders", notifyListeners);

        Assert.Contains("INSERT INTO \"orders\"", sql);
        Assert.Contains("(@messageId, 0, NOW(), 0, @payload)", sql);
        Assert.Equal(notifyListeners, sql.Contains("NOTIFY \"nymbroker_orders_changed\""));
    }

    [Fact]
    public void ClaimMessages_UsesSkipLocked_LiteralStatusesMatchingPartialIndex_AndReturnsPayload()
    {
        var sql = PostgresQueueSql.ClaimMessages("orders");

        Assert.Contains("FOR UPDATE SKIP LOCKED", sql);
        // Literals (not parameters) so the planner can prove the partial index predicate.
        Assert.Contains("WHERE status IN (0, 1)", sql);
        Assert.Contains("AND (status = 0 OR locked_until_utc <= NOW())", sql);
        Assert.Contains("ORDER BY created_at_utc, queue_id", sql);
        Assert.Contains("RETURNING m.queue_id, m.message_id, m.payload, m.attempt_count", sql);
        Assert.Contains("@leaseTimeout * INTERVAL '1 second'", sql);
    }

    [Fact]
    public void FinalizeMessages_WritesMixedOutcomesInOneStatement_GuardedByAttempt()
    {
        var sql = PostgresQueueSql.FinalizeMessages("orders");

        Assert.Contains("FROM unnest(@queueIds, @attempts, @statuses, @errors) AS source(queue_id, attempt, status, error)", sql);
        Assert.Contains("SET status = source.status", sql);
        Assert.Contains("completed_at_utc = CASE WHEN source.status = 2 THEN NOW() ELSE NULL END", sql);
        Assert.Contains("failed_at_utc = CASE WHEN source.status = 3 THEN NOW() ELSE NULL END", sql);
        Assert.Contains("AND target.status = 1", sql);
        Assert.Contains("AND target.attempt_count = source.attempt", sql);
    }

    [Fact]
    public void Listen_UsesNormalizedChannelName()
    {
        var sql = PostgresQueueSql.Listen("public.orders");

        Assert.Equal("LISTEN \"nymbroker_public_orders_changed\";", sql);
    }
}
