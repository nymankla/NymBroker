using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Endpoint.MySql;

namespace NymBroker.Tests;

/// <summary>
/// Behaviour tests for <see cref="MySqlEndPoint"/>. Unit and health-check tests always run; the rest need a real
/// MySQL and are skipped unless <c>NYMBROKER_MYSQL_CS</c> is set, e.g. after <c>scripts/setup-mysql.ps1</c>:
/// <c>Server=localhost;Port=3306;Database=nymbroker;User ID=root;Password=NymBroker!Dev123</c>
/// </summary>
public sealed class MySqlEndPointTests : IAsyncLifetime
{
    private const string ConnectionStringVariable = "NYMBROKER_MYSQL_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);
    private const string Unreachable = "Server=127.0.0.1;Port=1;Database=nymbroker;User ID=root;Password=x;Connection Timeout=1";

    // Unique per test instance so tests can run in parallel against one database.
    private readonly string _tableName = $"nb_test_{Guid.NewGuid():N}";
    private readonly List<MySqlEndPoint> _endpoints = [];

    // =====================================================================
    // Unit tests (always run)
    // =====================================================================

    [Theory]
    [InlineData("orders", "`orders`")]
    [InlineData("app.orders_queue", "`app`.`orders_queue`")]
    [InlineData("t$1", "`t$1`")]
    public void QuoteTableName_ValidNames_AreBacktickQuoted(string name, string quoted)
        => Assert.Equal(quoted, MySqlQueueSql.QuoteTableName(name));

    [Theory]
    [InlineData("")]
    [InlineData("a.b.c")]
    [InlineData("orders`; DROP TABLE x; --")]
    [InlineData("1orders")]
    [InlineData("db.")]
    public void QuoteTableName_InvalidNames_Throw(string name)
        => Assert.Throws<ArgumentException>(() => MySqlQueueSql.QuoteTableName(name));

    [Fact]
    public void AddMySqlEndPoint_InvalidTableName_ThrowsAtRegistration()
    {
        var builder = new ServiceCollection().AddNymBroker();
        Assert.Throws<ArgumentException>(() => builder.AddMySqlEndPoint("My", new MySqlSettings { TableName = "bad name" }));
    }

    [Fact]
    public async Task WithMySql_RegistersConfiguredEndpoint_CaseInsensitively()
    {
        const string json = """
            {
              "NymBroker": {
                "Endpoints": [
                  { "name": "My1", "type": "mysql", "config": { "connectionString": "Server=x", "tableName": "orders", "batchSize": 50 } }
                ]
              }
            }
            """;
        var path = Path.GetTempFileName();
        try
        {
            File.WriteAllText(path, json);
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddNymBroker().LoadConfiguration(path).WithMySql().Build();

            await using var sp = services.BuildServiceProvider();

            Assert.IsType<MySqlEndPoint>(sp.GetRequiredKeyedService<IEndPoint>("My1"));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task HealthCheck_UnreachableServer_ReturnsUnhealthyWithoutThrowing()
    {
        var logger = new CapturingLogger<MySqlEndPoint>();
        await using var ep = new MySqlEndPoint("Unreachable", new MySqlSettings { ConnectionString = Unreachable }, logger);

        var result = ep.HealthCheck();

        Assert.False(result.IsHealthy);
        Assert.False(string.IsNullOrEmpty(result.Message));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error);
    }

    [Fact]
    public async Task BrokerHealth_UnreachableServer_IsUnhealthy_AndNamesTheEndpoint()
    {
        var report = await BrokerHealthTestHelper.CheckAsync(b => b
            .AddMemoryEndPoint("Mem")
            .AddMySqlEndPoint("MyDown", new MySqlSettings { ConnectionString = Unreachable }, EndpointMode.WriteOnly));

        Assert.Equal(BrokerHealthStatus.Unhealthy, report.Status);
        Assert.Contains("MyDown", report.Message);
        Assert.Equal(BrokerHealthStatus.Healthy, report.Endpoints.Single(e => e.Name == "Mem").Status);
    }

    // =====================================================================
    // Integration tests (MySQL)
    // =====================================================================

    // --- Retention ---

    private MySqlEndPoint CreateRetentionEndPoint(CapturingLogger<MySqlEndPoint> logger)
    {
        var ep = new MySqlEndPoint("MySqlTest", new MySqlSettings
        {
            ConnectionString = ConnectionString!,
            TableName = _tableName,
            PollInterval = TimeSpan.FromMilliseconds(20),
            CompletedRetention = RetentionContract.CompletedRetention,
            FailedRetention = RetentionContract.FailedRetention,
            CleanupInterval = RetentionContract.CleanupInterval,
            CleanupBatchSize = RetentionContract.CleanupBatchSize
        }, logger);
        _endpoints.Add(ep);
        return ep;
    }

    [Fact]
    public async Task Retention_DeletesOnlyOldFinishedRows()
    {
        RequireMySql();
        var logger = new CapturingLogger<MySqlEndPoint>();
        await RetentionContract.DeletesOnlyOldFinishedRows(CreateRetentionEndPoint(logger), logger,
            (id, status, ageMinutes) =>
            {
                var finishedAt = $"UTC_TIMESTAMP(6) - INTERVAL {ageMinutes} MINUTE";
                return ExecuteAsync($"""
                    UPDATE {_tableName}
                       SET status = {(int)status},
                           created_at_utc = {finishedAt},
                           completed_at_utc = {(status == QueueMessageStatus.Completed ? finishedAt : "NULL")},
                           failed_at_utc = {(status == QueueMessageStatus.Failed ? finishedAt : "NULL")},
                           locked_until_utc = {(status == QueueMessageStatus.InProgress ? "UTC_TIMESTAMP(6) + INTERVAL 1 HOUR" : "NULL")}
                     WHERE queue_id = {id}
                    """);
            },
            () => ScalarAsync<string>($"SELECT COALESCE(GROUP_CONCAT(queue_id ORDER BY queue_id SEPARATOR ','), '') FROM {_tableName}"),
            () => ScalarAsync<long>($"SELECT COUNT(*) FROM {_tableName}_deferred"));
    }

    [Fact]
    public async Task Retention_TableFromAnOlderVersion_GetsTheIndex()
    {
        RequireMySql();
        await CreateEndPoint().PostAsync("{}"u8.ToArray(), Ct);
        await ExecuteAsync($"DROP INDEX ix_completed_at ON {_tableName}");

        await CreateEndPoint().PostAsync("{}"u8.ToArray(), Ct);

        Assert.Equal(1L, await ScalarAsync<long>(
            $"SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = '{_tableName}' AND index_name = 'ix_completed_at'"));
    }

    // --- Deferred ---

    [Fact]
    public async Task Deferred_MessageWaits_OtherMessagesDoNot()
    {
        RequireMySql();
        await DeferredContract.DeferredMessageWaits_OtherMessagesDoNot(CreateEndPoint());
    }

    [Fact]
    public async Task Deferred_RetryCountStartsAtTheFirstDelivery()
    {
        RequireMySql();
        var ep = CreateEndPoint(maxRetryCount: 2);
        await ep.PostDeferredAsync("{}"u8.ToArray(), DateTimeOffset.UtcNow.AddMilliseconds(200), TestContext.Current.CancellationToken);

        await ep.StartListeningAsync((_, _) => Task.FromResult(ProcessResult.Retry(description: "again")), TestContext.Current.CancellationToken);

        await WaitUntilAsync(async () => await CountAsync("status = 3 AND attempt_count = 2") == 1);
    }

    [Fact]
    public async Task Deferred_MoreDueThanABatch_EachIsDeliveredOnce()
    {
        RequireMySql();
        var ep = CreateEndPoint(batchSize: 10);
        var received = new ConcurrentBag<string>();
        for (var i = 0; i < 35; i++)
            await ep.PostDeferredAsync(Encoding.UTF8.GetBytes($"{{\"n\":{i}}}"), DateTimeOffset.UtcNow.AddMinutes(-1), TestContext.Current.CancellationToken);

        await ep.StartListeningAsync((raw, _) =>
        {
            received.Add(Encoding.UTF8.GetString(raw));
            return Task.FromResult(ProcessResult.Completed);
        }, TestContext.Current.CancellationToken);

        await WaitUntilAsync(() => Task.FromResult(received.Count >= 35));
        await Task.Delay(200, TestContext.Current.CancellationToken);
        Assert.Equal(35, received.Count);
        Assert.Equal(35, received.Distinct().Count());
    }

    [Fact]
    public async Task Deferred_TwoListeners_DeliverEachMessageOnce()
    {
        RequireMySql();
        var first = CreateEndPoint();
        var second = CreateEndPoint();
        var received = new ConcurrentBag<string>();
        Func<byte[], CancellationToken, Task<ProcessResult>> handler = (raw, _) =>
        {
            received.Add(Encoding.UTF8.GetString(raw));
            return Task.FromResult(ProcessResult.Completed);
        };
        for (var i = 0; i < 40; i++)
            await first.PostDeferredAsync(Encoding.UTF8.GetBytes($"{{\"n\":{i}}}"), DateTimeOffset.UtcNow.AddMilliseconds(300), TestContext.Current.CancellationToken);

        await first.StartListeningAsync(handler, TestContext.Current.CancellationToken);
        await second.StartListeningAsync(handler, TestContext.Current.CancellationToken);

        await WaitUntilAsync(() => Task.FromResult(received.Count >= 40));
        await Task.Delay(300, TestContext.Current.CancellationToken);
        Assert.Equal(40, received.Count);
        Assert.Equal(40, received.Distinct().Count());
    }

    [Fact]
    public async Task PostedMessage_ReachesHandler_WithBytesUnchanged_AndRowIsCompleted()
    {
        RequireMySql();
        var ep = CreateEndPoint();
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var payload = Encoding.UTF8.GetBytes("""{"id":"1","message":{"text":"åäö"}}""");

        await ep.PostAsync(payload, Ct);
        await ep.StartListeningAsync((raw, _) => { received.TrySetResult(raw); return Task.FromResult(ProcessResult.Completed); }, Ct);

        Assert.Equal(payload, await received.Task.WaitAsync(Timeout, Ct));
        await WaitUntilAsync(async () => await CountAsync("status = 2 AND completed_at_utc IS NOT NULL AND locked_until_utc IS NULL") == 1);
    }

    [Fact]
    public async Task ThrowingHandler_ReturnsToPending_ThenFailed_AndNextMessageIsStillProcessed()
    {
        RequireMySql();
        var logger = new CapturingLogger<MySqlEndPoint>();
        var ep = CreateEndPoint(logger, maxRetryCount: 2);
        var ok = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var boomAttempts = 0;

        await ep.PostAsync("boom"u8.ToArray(), Ct);
        await ep.PostAsync("ok"u8.ToArray(), Ct);
        await ep.StartListeningAsync((raw, _) =>
        {
            if (Encoding.UTF8.GetString(raw) == "boom")
            {
                Interlocked.Increment(ref boomAttempts);
                throw new InvalidOperationException("boom failed");
            }
            ok.TrySetResult();
            return Task.FromResult(ProcessResult.Completed);
        }, Ct);

        await ok.Task.WaitAsync(Timeout, Ct);
        await WaitUntilAsync(async () => await CountAsync("status = 3 AND attempt_count = 2 AND last_error = 'boom failed' AND failed_at_utc IS NOT NULL") == 1);

        Assert.Equal(2, Volatile.Read(ref boomAttempts));
        Assert.Equal(1, await CountAsync("status = 2"));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error && e.Message.Contains("Unhandled error dispatching"));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("marked Failed after 2 attempts"));
    }

    [Fact]
    public async Task PollError_IsLogged_AndLoopRecovers()
    {
        RequireMySql();
        var logger = new CapturingLogger<MySqlEndPoint>();
        var ep = CreateEndPoint(logger);
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        Assert.True(ep.HealthCheck().IsHealthy);   // also creates the table
        await ep.StartListeningAsync((raw, _) => { received.TrySetResult(Encoding.UTF8.GetString(raw)); return Task.FromResult(ProcessResult.Completed); }, Ct);

        await ExecuteAsync($"DROP TABLE {_tableName}");
        await WaitUntilAsync(() => Task.FromResult(logger.Entries.Any(e => e.Level == LogLevel.Error && e.Message.Contains("Poll error"))));

        // A fresh endpoint on the same table recreates the schema; the original loop must pick the row up.
        await CreateEndPoint().PostAsync("after-recovery"u8.ToArray(), Ct);

        Assert.Equal("after-recovery", await received.Task.WaitAsync(Timeout, Ct));
        Assert.DoesNotContain(logger.Entries, e => e.Level == LogLevel.Critical);
    }

    [Fact]
    public async Task AfterStopListening_HandlerIsNeverCalled_AndRowStaysPending()
    {
        RequireMySql();
        var ep = CreateEndPoint();
        var calls = 0;
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.StartListeningAsync((_, _) => { Interlocked.Increment(ref calls); first.TrySetResult(); return Task.FromResult(ProcessResult.Completed); }, Ct);
        await ep.PostAsync("before-stop"u8.ToArray(), Ct);
        await first.Task.WaitAsync(Timeout, Ct);

        await ep.StopListeningAsync();
        var callsAtStop = Volatile.Read(ref calls);
        await ep.PostAsync("after-stop"u8.ToArray(), Ct);
        await Task.Delay(500, Ct);

        Assert.Equal(callsAtStop, Volatile.Read(ref calls));
        Assert.Equal(1, await CountAsync("status = 0"));
    }

    [Fact]
    public async Task StopListening_FinalizesMessagesAlreadyHandled()
    {
        RequireMySql();
        var ep = CreateEndPoint();
        var fastHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSlow = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.PostAsync("fast"u8.ToArray(), Ct);
        await ep.PostAsync("slow"u8.ToArray(), Ct);
        await ep.StartListeningAsync(async (raw, _) =>
        {
            if (Encoding.UTF8.GetString(raw) == "fast") { fastHandled.TrySetResult(); return ProcessResult.Completed; }
            await releaseSlow.Task;   // still running when stop is requested; completes normally
            return ProcessResult.Completed;
        }, Ct);

        await fastHandled.Task.WaitAsync(Timeout, Ct);
        var stop = ep.StopListeningAsync();
        releaseSlow.SetResult();
        await stop;

        Assert.Equal(2, await CountAsync("status = 2"));
        Assert.Equal(0, await CountAsync("status = 1"));
    }

    [Fact]
    public async Task DeadLetterResult_MarksRowFailedImmediately_WithReason()
    {
        RequireMySql();
        var ep = CreateEndPoint(maxRetryCount: 5);
        await ep.PostAsync("x"u8.ToArray(), Ct);
        var calls = 0;

        await ep.StartListeningAsync((_, _) =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(ProcessResult.DeadLetter(DeadLetterReasons.Expired, "too old"));
        }, Ct);

        await WaitUntilAsync(async () => await CountAsync("status = 3 AND attempt_count = 1 AND last_error = 'Expired: too old'") == 1);
        await ep.StopListeningAsync();

        Assert.Equal(1, Volatile.Read(ref calls));
    }

    [Fact]
    public async Task ExpiredLease_IsReclaimed_AndProcessed()
    {
        RequireMySql();
        var ep = CreateEndPoint();
        await ep.PostAsync("orphan"u8.ToArray(), Ct);
        // A poller claimed it (attempt 1) and died: the lease ran out a minute ago.
        await ExecuteAsync($"UPDATE {_tableName} SET status = 1, attempt_count = 1, locked_until_utc = UTC_TIMESTAMP(6) - INTERVAL 1 MINUTE");
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.StartListeningAsync((raw, _) => { received.TrySetResult(Encoding.UTF8.GetString(raw)); return Task.FromResult(ProcessResult.Completed); }, Ct);

        Assert.Equal("orphan", await received.Task.WaitAsync(Timeout, Ct));
        await WaitUntilAsync(async () => await CountAsync("status = 2 AND attempt_count = 2") == 1);
    }

    [Fact]
    public async Task LiveLease_IsNotReclaimed()
    {
        RequireMySql();
        var ep = CreateEndPoint();
        await ep.PostAsync("owned"u8.ToArray(), Ct);
        await ExecuteAsync($"UPDATE {_tableName} SET status = 1, attempt_count = 1, locked_until_utc = UTC_TIMESTAMP(6) + INTERVAL 5 MINUTE");
        var calls = 0;

        await ep.StartListeningAsync((_, _) => { Interlocked.Increment(ref calls); return Task.FromResult(ProcessResult.Completed); }, Ct);
        await Task.Delay(500, Ct);

        Assert.Equal(0, Volatile.Read(ref calls));
        Assert.Equal(1, await CountAsync("status = 1 AND attempt_count = 1"));
    }

    [Fact]
    public async Task Finalize_WithStaleAttempt_DoesNotOverwriteReclaimedRow()
    {
        RequireMySql();
        var ep = CreateEndPoint();
        await ep.PostAsync("x"u8.ToArray(), Ct);
        // Claimed twice (the lease expired once); the current owner holds attempt 2.
        await ExecuteAsync($"UPDATE {_tableName} SET status = 1, attempt_count = 2, locked_until_utc = UTC_TIMESTAMP(6) + INTERVAL 5 MINUTE");
        var queueId = await ScalarAsync<long>($"SELECT queue_id FROM {_tableName}");

        // The first owner (attempt 1) finishes late and tries to mark it Completed.
        await using (var conn = await OpenAsync())
        {
            await using var cmd = new MySqlCommand(MySqlQueueSql.Finalize(MySqlQueueSql.QuoteTableName(_tableName)), conn);
            cmd.Parameters.AddWithValue("@items", $$"""[{"id":{{queueId}},"attempt":1,"status":2,"error":null}]""");
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        Assert.Equal(1, await CountAsync("status = 1 AND attempt_count = 2"));
    }

    [Fact]
    public async Task Finalize_NullError_IsStoredAsNull()
    {
        RequireMySql();
        var ep = CreateEndPoint();
        await ep.PostAsync("x"u8.ToArray(), Ct);
        await ExecuteAsync($"UPDATE {_tableName} SET status = 1, attempt_count = 1, last_error = 'old'");
        var queueId = await ScalarAsync<long>($"SELECT queue_id FROM {_tableName}");

        await using (var conn = await OpenAsync())
        {
            await using var cmd = new MySqlCommand(MySqlQueueSql.Finalize(MySqlQueueSql.QuoteTableName(_tableName)), conn);
            cmd.Parameters.AddWithValue("@items", $$"""[{"id":{{queueId}},"attempt":1,"status":2,"error":null}]""");
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        Assert.Equal(1, await CountAsync("status = 2 AND last_error IS NULL"));
    }

    [Fact]
    public async Task TwoEndpointsOnOneTable_ProcessEachMessageExactlyOnce()
    {
        RequireMySql();
        const int messageCount = 60;
        var seen = new ConcurrentDictionary<string, int>();
        var producer = CreateEndPoint();
        for (var i = 0; i < messageCount; i++)
            await producer.PostAsync(Encoding.UTF8.GetBytes($"m{i}"), Ct);

        Task<ProcessResult> Handler(byte[] raw, CancellationToken _)
        {
            seen.AddOrUpdate(Encoding.UTF8.GetString(raw), 1, (_, n) => n + 1);
            return Task.FromResult(ProcessResult.Completed);
        }

        await CreateEndPoint(batchSize: 5).StartListeningAsync(Handler, Ct);
        await CreateEndPoint(batchSize: 5).StartListeningAsync(Handler, Ct);

        await WaitUntilAsync(async () => await CountAsync("status = 2") == messageCount);
        Assert.Equal(messageCount, seen.Count);
        Assert.All(seen.Values, n => Assert.Equal(1, n));
    }

    [Fact]
    public async Task PostBatch_InsertsAllRows_AndTheyAreDeliveredOneByOne_InOrder()
    {
        RequireMySql();
        var ep = CreateEndPoint(batchSize: 3);
        var sent = Enumerable.Range(0, 20).Select(i => $"m{i:00}").ToArray();
        var received = new ConcurrentQueue<byte[]>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.PostBatchAsync(sent.Select(s => Encoding.UTF8.GetBytes(s)).ToList(), Ct);
        await ep.StartListeningAsync((raw, _) =>
        {
            received.Enqueue(raw);
            if (received.Count == sent.Length) done.TrySetResult();
            return Task.FromResult(ProcessResult.Completed);
        }, Ct);

        await done.Task.WaitAsync(Timeout, Ct);
        await ep.StopListeningAsync();

        Assert.Equal(sent, received.Select(b => Encoding.UTF8.GetString(b)));
    }

    [Fact]
    public async Task PostBatch_IsAtomic_AFailingRowInsertsNothing()
    {
        RequireMySql();
        var ep = CreateEndPoint();
        Assert.True(ep.HealthCheck().IsHealthy);   // creates the table
        // "boom" is the only 4-byte payload in the batch.
        await ExecuteAsync($"ALTER TABLE {_tableName} ADD CONSTRAINT ck_no_boom_{Guid.NewGuid():N} CHECK (LENGTH(payload) <> 4)");

        await Assert.ThrowsAnyAsync<Exception>(() => ep.PostBatchAsync(["first"u8.ToArray(), "boom"u8.ToArray(), "second"u8.ToArray()], Ct));

        Assert.Equal(0, await CountAsync("1 = 1"));
    }

    [Fact]
    public async Task BinaryPayloads_RoundTripUnchanged()
    {
        RequireMySql();
        var ep = CreateEndPoint();
        var payload = Enumerable.Range(0, 1024).Select(i => (byte)i).ToArray();   // every byte value, incl. 0 and invalid UTF-8
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.PostBatchAsync([payload], Ct);
        await ep.StartListeningAsync((raw, _) => { received.TrySetResult(raw); return Task.FromResult(ProcessResult.Completed); }, Ct);

        Assert.Equal(payload, await received.Task.WaitAsync(Timeout, Ct));
    }

    [Fact]
    public async Task Claim_ReadsOnlyThePendingRangeOfTheStatusIndex()
    {
        RequireMySql();
        var ep = CreateEndPoint();
        Assert.True(ep.HealthCheck().IsHealthy);   // creates the table
        var table = MySqlQueueSql.QuoteTableName(_tableName);

        var plan = await PlanAsync($"SELECT queue_id FROM {table} WHERE status = 0 ORDER BY queue_id LIMIT 10");

        Assert.Equal("ix_status_queue", plan["key"]);
        Assert.DoesNotContain("filesort", plan["Extra"] ?? "");
    }

    [Fact]
    public async Task Finalize_FindsRowsByPrimaryKey()
    {
        RequireMySql();
        var ep = CreateEndPoint();
        // A realistic table: mostly finished rows, a few in progress (with only Pending rows, a scan of the empty
        // status = 1 index range is just as cheap and the optimizer may pick it).
        await ep.PostBatchAsync([.. Enumerable.Range(0, 2000).Select(i => Encoding.UTF8.GetBytes($"m{i}"))], Ct);
        await ExecuteAsync($"UPDATE {_tableName} SET status = IF(queue_id % 100 = 0, 1, 2)");
        await ExecuteAsync($"ANALYZE TABLE {_tableName}");
        var table = MySqlQueueSql.QuoteTableName(_tableName);

        var plans = await PlansAsync($"EXPLAIN {MySqlQueueSql.Finalize(table).Replace("@items", """'[{"id":1,"attempt":1,"status":2,"error":null}]'""")}");

        var target = plans.Single(p => p["table"] == "target");
        Assert.Equal("PRIMARY", target["key"]);
        Assert.Equal("eq_ref", target["type"]);
    }

    [Fact]
    public async Task BrokerHealth_RealServer_IsHealthy()
    {
        RequireMySql();
        var report = await BrokerHealthTestHelper.CheckAsync(b => b
            .AddMySqlEndPoint("My", new MySqlSettings { ConnectionString = ConnectionString!, TableName = _tableName }, EndpointMode.WriteOnly));

        Assert.Equal(BrokerHealthStatus.Healthy, report.Status);
    }

    // =====================================================================
    // helpers
    // =====================================================================

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static void RequireMySql()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString),
            $"Set {ConnectionStringVariable} to run MySQL integration tests (scripts/setup-mysql.ps1).");

    private MySqlEndPoint CreateEndPoint(CapturingLogger<MySqlEndPoint>? logger = null, int maxRetryCount = 5, int batchSize = 10)
    {
        var ep = new MySqlEndPoint("MySqlTest", new MySqlSettings
        {
            ConnectionString = ConnectionString!,
            TableName        = _tableName,
            PollInterval     = TimeSpan.FromMilliseconds(20),
            MaxRetryCount    = maxRetryCount,
            BatchSize        = batchSize
        }, logger ?? new CapturingLogger<MySqlEndPoint>());
        _endpoints.Add(ep);
        return ep;
    }

    private static async Task<MySqlConnection> OpenAsync()
    {
        var conn = new MySqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        return conn;
    }

    private async Task<long> CountAsync(string where)
        => await ScalarAsync<long>($"SELECT COUNT(*) FROM {_tableName} WHERE {where}");

    private static async Task<T> ScalarAsync<T>(string sql)
    {
        await using var conn = await OpenAsync();
        await using var cmd = new MySqlCommand(sql, conn);
        return (T)Convert.ChangeType((await cmd.ExecuteScalarAsync(Ct))!, typeof(T));
    }

    private static async Task ExecuteAsync(string sql)
    {
        await using var conn = await OpenAsync();
        await using var cmd = new MySqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static async Task<Dictionary<string, string?>> PlanAsync(string select)
        => Assert.Single(await PlansAsync($"EXPLAIN {select}"));

    private static async Task<List<Dictionary<string, string?>>> PlansAsync(string explain)
    {
        await using var conn = await OpenAsync();
        await using var cmd = new MySqlCommand(explain, conn);
        await using var reader = await cmd.ExecuteReaderAsync(Ct);
        var rows = new List<Dictionary<string, string?>>();
        while (await reader.ReadAsync(Ct))
            rows.Add(Enumerable.Range(0, reader.FieldCount).ToDictionary(reader.GetName, i => reader.IsDBNull(i) ? null : reader.GetValue(i).ToString()));
        return rows;
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Condition was not met in time.");
            await Task.Delay(25, Ct);
        }
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var ep in _endpoints)
            await ep.DisposeAsync();

        if (!string.IsNullOrWhiteSpace(ConnectionString))
        {
            await ExecuteAsync($"DROP TABLE IF EXISTS {_tableName}_deferred");
            await ExecuteAsync($"DROP TABLE IF EXISTS {_tableName}");
        }
    }
}
