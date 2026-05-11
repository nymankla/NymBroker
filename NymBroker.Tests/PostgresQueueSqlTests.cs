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
    public void CreateSchema_UsesByteaPayload_AndIndexes()
    {
        var sql = PostgresQueueSql.CreateSchema("public.orders");

        Assert.Contains("payload          BYTEA       NOT NULL", sql);
        Assert.Contains("CREATE TABLE IF NOT EXISTS \"public\".\"orders\"", sql);
        Assert.Contains("CREATE INDEX IF NOT EXISTS \"ix_public_orders_status_created\"", sql);
        Assert.Contains("CREATE INDEX IF NOT EXISTS \"ix_public_orders_status_locked_until\"", sql);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void InsertMessage_OptionallyAddsNotify(bool notifyListeners)
    {
        var sql = PostgresQueueSql.InsertMessage("orders", notifyListeners);

        Assert.Contains("INSERT INTO \"orders\"", sql);
        Assert.Contains("@payload", sql);
        Assert.Equal(notifyListeners, sql.Contains("NOTIFY \"nymbroker_orders_changed\""));
    }

    [Fact]
    public void ClaimMessages_UsesSkipLocked_AndReturnsPayload()
    {
        var sql = PostgresQueueSql.ClaimMessages("orders");

        Assert.Contains("FOR UPDATE SKIP LOCKED", sql);
        Assert.Contains("RETURNING m.queue_id, m.message_id, m.payload, m.attempt_count", sql);
        Assert.Contains("@leaseTimeout * INTERVAL '1 second'", sql);
    }

    [Fact]
    public void FinalizeCompleted_UsesArrayParameter()
    {
        var sql = PostgresQueueSql.FinalizeCompleted("orders");

        Assert.Contains("WHERE queue_id = ANY(@queueIds)", sql);
        Assert.Contains("completed_at_utc = NOW()", sql);
    }

    [Fact]
    public void FinalizeWithErrors_UsesUnnestForBatchedUpdates()
    {
        var sql = PostgresQueueSql.FinalizeWithErrors("orders");

        Assert.Contains("FROM unnest(@queueIds, @errors)", sql);
        Assert.Contains("last_error = source.error", sql);
        Assert.Contains("CASE WHEN @status = @failedStatus THEN NOW() ELSE NULL END", sql);
    }

    [Fact]
    public void Listen_UsesNormalizedChannelName()
    {
        var sql = PostgresQueueSql.Listen("public.orders");

        Assert.Equal("LISTEN \"nymbroker_public_orders_changed\";", sql);
    }
}
