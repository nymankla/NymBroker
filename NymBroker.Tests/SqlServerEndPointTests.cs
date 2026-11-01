using System.Collections.Concurrent;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using NymBroker.Endpoint.SqlServer;
using NymBroker.Core.Endpoint;
using Microsoft.Extensions.DependencyInjection;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Queue;

namespace NymBroker.Tests;

/// <summary>
/// Behaviour tests for <see cref="SqlServerEndPoint"/>. The health-check test always runs; the rest need a
/// real SQL Server and are skipped unless <c>NYMBROKER_SQLSERVER_CS</c> is set, e.g. after
/// <c>scripts/setup-sqlserver.ps1</c>:
/// <c>Server=localhost,1433;Database=nymbroker;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True</c>
/// </summary>
public sealed class SqlServerEndPointTests : IAsyncLifetime
{
    private const string ConnectionStringVariable = "NYMBROKER_SQLSERVER_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);

    // Unique per test instance so tests can run in parallel against one database.
    private readonly string _tableName = $"dbo.nb_test_{Guid.NewGuid():N}";
    private readonly List<SqlServerEndPoint> _endpoints = [];

    // --- 5. Health check (always runs) ---

    [Fact]
    public async Task HealthCheck_UnreachableServer_ReturnsUnhealthyWithoutThrowing()
    {
        var logger = new CapturingLogger<SqlServerEndPoint>();
        await using var ep = new SqlServerEndPoint("Unreachable",
            new SqlServerSettings { ConnectionString = "Server=127.0.0.1,1;Database=nymbroker;User Id=sa;Password=x;Connect Timeout=1;TrustServerCertificate=True" },
            logger);

        var result = ep.HealthCheck();

        Assert.False(result.IsHealthy);
        Assert.False(string.IsNullOrEmpty(result.Message));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error);
    }

    // --- Retention ---

    [Fact]
    public async Task Retention_DeletesOnlyOldFinishedRows()
    {
        RequireSqlServer();
        var logger = new CapturingLogger<SqlServerEndPoint>();
        var ep = new SqlServerEndPoint("SqlTest", new SqlServerSettings
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

        await RetentionContract.DeletesOnlyOldFinishedRows(ep, logger,
            (id, status, ageMinutes) =>
            {
                var finishedAt = $"DATEADD(MINUTE, -{ageMinutes}, SYSUTCDATETIME())";
                return ExecuteAsync($"""
                    UPDATE {_tableName}
                       SET status = {(int)status},
                           created_at_utc = {finishedAt},
                           completed_at_utc = {(status == QueueMessageStatus.Completed ? finishedAt : "NULL")},
                           failed_at_utc = {(status == QueueMessageStatus.Failed ? finishedAt : "NULL")},
                           locked_until_utc = {(status == QueueMessageStatus.InProgress ? "DATEADD(HOUR, 1, SYSUTCDATETIME())" : "NULL")}
                     WHERE queue_id = {id}
                    """);
            },
            () => ScalarAsync<string>($"SELECT COALESCE(STRING_AGG(CAST(queue_id AS VARCHAR(20)), ',') WITHIN GROUP (ORDER BY queue_id), '') FROM {_tableName}"),
            async () => await ScalarAsync<int>($"SELECT COUNT(*) FROM {_tableName}_deferred"));

        var safeName = _tableName.Replace('.', '_');
        Assert.Equal(2, await ScalarAsync<int>(
            $"SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID(N'{_tableName}') AND name IN ('ix_{safeName}_completed', 'ix_{safeName}_failed') AND has_filter = 1"));
    }

    // --- Deferred ---

    [Fact]
    public async Task Deferred_MessageWaits_OtherMessagesDoNot()
    {
        RequireSqlServer();
        await DeferredContract.DeferredMessageWaits_OtherMessagesDoNot(CreateEndPoint());
    }

    [Fact]
    public async Task Deferred_RetryCountStartsAtTheFirstDelivery()
    {
        RequireSqlServer();
        var ep = CreateEndPoint(maxRetryCount: 2);
        await ep.PostDeferredAsync("{}"u8.ToArray(), DateTimeOffset.UtcNow.AddMilliseconds(200), TestContext.Current.CancellationToken);

        await ep.StartListeningAsync((_, _) => Task.FromResult(ProcessResult.Retry(description: "again")), TestContext.Current.CancellationToken);

        await WaitUntilAsync(async () => await CountAsync("status = 3 AND attempt_count = 2") == 1);
    }

    [Fact]
    public async Task Deferred_MoreDueThanABatch_EachIsDeliveredOnce()
    {
        RequireSqlServer();
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
        RequireSqlServer();
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

    // --- 1. Round trip ---

    [Fact]
    public async Task PostedMessage_ReachesHandler_WithBytesUnchanged_AndRowIsCompleted()
    {
        RequireSqlServer();
        var ep = CreateEndPoint();
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var payload = Encoding.UTF8.GetBytes("""{"id":"1","message":{"text":"åäö"}}""");

        await ep.PostAsync(payload, TestContext.Current.CancellationToken);
        await ep.StartListeningAsync((raw, _) => { received.TrySetResult(raw); return Task.FromResult(ProcessResult.Completed); },
            TestContext.Current.CancellationToken);

        Assert.Equal(payload, await received.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken));
        await WaitUntilAsync(async () => await CountAsync("status = 2") == 1);
    }

    // --- 2. Handler failure: retried, then Failed; other messages still processed ---

    [Fact]
    public async Task ThrowingHandler_ReturnsToPending_ThenFailed_AndNextMessageIsStillProcessed()
    {
        RequireSqlServer();
        var logger = new CapturingLogger<SqlServerEndPoint>();
        var ep = CreateEndPoint(logger, maxRetryCount: 2);
        var ok = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var boomAttempts = 0;

        await ep.PostAsync(Encoding.UTF8.GetBytes("boom"), TestContext.Current.CancellationToken);
        await ep.PostAsync(Encoding.UTF8.GetBytes("ok"), TestContext.Current.CancellationToken);
        await ep.StartListeningAsync((raw, _) =>
        {
            if (Encoding.UTF8.GetString(raw) == "boom")
            {
                Interlocked.Increment(ref boomAttempts);
                throw new InvalidOperationException("boom failed");
            }
            ok.TrySetResult();
            return Task.FromResult(ProcessResult.Completed);
        }, TestContext.Current.CancellationToken);

        await ok.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);
        await WaitUntilAsync(async () => await CountAsync("status = 3 AND attempt_count = 2 AND last_error = N'boom failed'") == 1);

        Assert.Equal(2, Volatile.Read(ref boomAttempts));
        Assert.Equal(1, await CountAsync("status = 2"));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error && e.Message.Contains("Unhandled error dispatching"));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("marked Failed after 2 attempts"));
    }

    // --- 3. Poll error is logged and the loop recovers ---

    [Fact]
    public async Task PollError_IsLogged_AndLoopRecovers()
    {
        RequireSqlServer();
        var logger = new CapturingLogger<SqlServerEndPoint>();
        var ep = CreateEndPoint(logger);
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        Assert.True(ep.HealthCheck().IsHealthy);   // also creates the table
        await ep.StartListeningAsync((raw, _) => { received.TrySetResult(Encoding.UTF8.GetString(raw)); return Task.FromResult(ProcessResult.Completed); },
            TestContext.Current.CancellationToken);

        await ExecuteAsync($"DROP TABLE {_tableName}");
        await WaitUntilAsync(() => Task.FromResult(logger.Entries.Any(e => e.Level == LogLevel.Error && e.Message.Contains("Poll error"))));

        // A fresh endpoint on the same table recreates the schema; the original loop must pick the row up.
        await CreateEndPoint().PostAsync(Encoding.UTF8.GetBytes("after-recovery"), TestContext.Current.CancellationToken);

        Assert.Equal("after-recovery", await received.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken));
        Assert.DoesNotContain(logger.Entries, e => e.Level == LogLevel.Critical);
    }

    // --- 4. No handler calls after StopListeningAsync returns ---

    [Fact]
    public async Task AfterStopListening_HandlerIsNeverCalled_AndRowStaysPending()
    {
        RequireSqlServer();
        var ep = CreateEndPoint();
        var calls = 0;
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.StartListeningAsync((_, _) => { Interlocked.Increment(ref calls); first.TrySetResult(); return Task.FromResult(ProcessResult.Completed); },
            TestContext.Current.CancellationToken);
        await ep.PostAsync(Encoding.UTF8.GetBytes("before-stop"), TestContext.Current.CancellationToken);
        await first.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        await ep.StopListeningAsync();
        var callsAtStop = Volatile.Read(ref calls);
        await ep.PostAsync(Encoding.UTF8.GetBytes("after-stop"), TestContext.Current.CancellationToken);
        await Task.Delay(500, TestContext.Current.CancellationToken);

        Assert.Equal(callsAtStop, Volatile.Read(ref calls));
        Assert.Equal(1, await CountAsync("status = 0"));
    }

    // --- Results of handled messages are written on shutdown, not left InProgress ---

    [Fact]
    public async Task StopListening_FinalizesMessagesAlreadyHandled()
    {
        RequireSqlServer();
        var ep = CreateEndPoint();
        var fastHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSlow = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.PostAsync(Encoding.UTF8.GetBytes("fast"), TestContext.Current.CancellationToken);
        await ep.PostAsync(Encoding.UTF8.GetBytes("slow"), TestContext.Current.CancellationToken);
        await ep.StartListeningAsync(async (raw, _) =>
        {
            if (Encoding.UTF8.GetString(raw) == "fast") { fastHandled.TrySetResult(); return ProcessResult.Completed; }
            await releaseSlow.Task;   // still running when stop is requested; completes normally
            return ProcessResult.Completed;
        }, TestContext.Current.CancellationToken);

        await fastHandled.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);
        var stop = ep.StopListeningAsync();   // cancels the loop, then waits for it
        releaseSlow.SetResult();
        await stop;

        Assert.Equal(2, await CountAsync("status = 2"));
        Assert.Equal(0, await CountAsync("status = 1"));
    }

    // --- ProcessResult.DeadLetter marks the row Failed immediately, with the reason ---

    [Fact]
    public async Task DeadLetterResult_MarksRowFailedImmediately_WithReason()
    {
        RequireSqlServer();
        var ep = CreateEndPoint(maxRetryCount: 5);
        await ep.PostAsync(Encoding.UTF8.GetBytes("x"), TestContext.Current.CancellationToken);

        var calls = 0;
        await ep.StartListeningAsync((_, _) =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(ProcessResult.DeadLetter(DeadLetterReasons.Expired, "too old"));
        }, TestContext.Current.CancellationToken);

        await WaitUntilAsync(async () => await CountAsync("status = 3 AND attempt_count = 1 AND last_error = N'Expired: too old'") == 1);
        await ep.StopListeningAsync();

        Assert.Equal(1, Volatile.Read(ref calls));
    }

    // --- A late finalize (stale attempt) must not overwrite a re-claimed row ---

    [Fact]
    public async Task Finalize_WithStaleAttempt_DoesNotOverwriteReclaimedRow()
    {
        RequireSqlServer();
        var ep = CreateEndPoint();
        await ep.PostAsync(Encoding.UTF8.GetBytes("x"), TestContext.Current.CancellationToken);
        // Simulate: claimed twice (lease expired once), the current owner holds attempt 2.
        await ExecuteAsync($"UPDATE {_tableName} SET status = 1, attempt_count = 2, locked_until_utc = DATEADD(MINUTE, 5, SYSUTCDATETIME())");
        var queueId = await ScalarAsync<long>($"SELECT queue_id FROM {_tableName}");

        // The first owner (attempt 1) finishes late and tries to mark it Completed.
        await using (var conn = new SqlConnection(ConnectionString))
        {
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = SqlServerQueueSql.Finalize(_tableName);
            cmd.Parameters.AddWithValue("@items", $$"""[{"id":{{queueId}},"attempt":1,"status":2,"error":null}]""");
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        Assert.Equal(1, await CountAsync("status = 1 AND attempt_count = 2"));
    }

    // --- Competing consumers: UPDLOCK, READPAST prevents double-processing ---

    [Fact]
    public async Task TwoEndpointsOnOneTable_ProcessEachMessageExactlyOnce()
    {
        RequireSqlServer();
        const int messageCount = 60;
        var seen = new ConcurrentDictionary<string, int>();
        var producer = CreateEndPoint();
        for (var i = 0; i < messageCount; i++)
            await producer.PostAsync(Encoding.UTF8.GetBytes($"m{i}"), TestContext.Current.CancellationToken);

        Task<ProcessResult> Handler(byte[] raw, CancellationToken _)
        {
            seen.AddOrUpdate(Encoding.UTF8.GetString(raw), 1, (_, n) => n + 1);
            return Task.FromResult(ProcessResult.Completed);
        }

        await CreateEndPoint(batchSize: 5).StartListeningAsync(Handler, TestContext.Current.CancellationToken);
        await CreateEndPoint(batchSize: 5).StartListeningAsync(Handler, TestContext.Current.CancellationToken);

        await WaitUntilAsync(async () => await CountAsync("status = 2") == messageCount);
        Assert.Equal(messageCount, seen.Count);
        Assert.All(seen.Values, n => Assert.Equal(1, n));
    }

    // --- PostBatchAsync: one insert, order preserved, delivered one by one ---

    [Fact]
    public async Task PostBatch_InsertsAllRows_AndTheyAreDeliveredOneByOne_InOrder()
    {
        RequireSqlServer();
        var ep = CreateEndPoint();
        var sent = new[] { "first", "second", "third", "fourth" };
        var received = new ConcurrentQueue<byte[]>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.PostBatchAsync(sent.Select(s => Encoding.UTF8.GetBytes(s)).ToList(), TestContext.Current.CancellationToken);
        await ep.StartListeningAsync((raw, _) =>
        {
            received.Enqueue(raw);
            if (received.Count == sent.Length) done.TrySetResult();
            return Task.FromResult(ProcessResult.Completed);
        }, TestContext.Current.CancellationToken);

        await done.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);
        await ep.StopListeningAsync();

        Assert.Equal(sent, received.Select(b => Encoding.UTF8.GetString(b)));
    }

    [Fact]
    public async Task PostBatch_IsAtomic_AFailingRowInsertsNothing()
    {
        RequireSqlServer();
        var ep = CreateEndPoint();
        Assert.True(ep.HealthCheck().IsHealthy);   // creates the table
        // "boom" is the only 4-byte payload in the batch.
        await ExecuteAsync($"ALTER TABLE {_tableName} ADD CONSTRAINT ck_no_boom CHECK (DATALENGTH(payload) <> 4)");

        await Assert.ThrowsAnyAsync<Exception>(() => ep.PostBatchAsync(
            ["first"u8.ToArray(), "boom"u8.ToArray(), "second"u8.ToArray()], TestContext.Current.CancellationToken));

        Assert.Equal(0, await CountAsync("1 = 1"));
    }

    [Fact]
    public async Task PostBatch_BinaryPayloads_RoundTripUnchanged()
    {
        RequireSqlServer();
        var ep = CreateEndPoint();
        var payload = Enumerable.Range(0, 1024).Select(i => (byte)i).ToArray();   // every byte value, incl. 0 and invalid UTF-8
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.PostBatchAsync([payload], TestContext.Current.CancellationToken);
        await ep.StartListeningAsync((raw, _) => { received.TrySetResult(raw); return Task.FromResult(ProcessResult.Completed); },
            TestContext.Current.CancellationToken);

        Assert.Equal(payload, await received.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken));
    }

    // --- 6. Broker health check (#53) ---

    [Fact]
    public async Task BrokerHealth_UnreachableServer_IsUnhealthy_AndNamesTheEndpoint()
    {
        var report = await BrokerHealthTestHelper.CheckAsync(b => b
            .AddMemoryEndPoint("Mem")
            .AddSqlServerEndPoint("SqlDown", new SqlServerSettings
            {
                ConnectionString = "Server=127.0.0.1,1;Database=nymbroker;User Id=sa;Password=x;Connect Timeout=1;TrustServerCertificate=True"
            }, EndpointMode.WriteOnly));

        Assert.Equal(BrokerHealthStatus.Unhealthy, report.Status);
        Assert.Contains("SqlDown", report.Message);
        Assert.Equal(BrokerHealthStatus.Unhealthy, report.Endpoints.Single(e => e.Name == "SqlDown").Status);
        Assert.Equal(BrokerHealthStatus.Healthy, report.Endpoints.Single(e => e.Name == "Mem").Status);
    }

    [Fact]
    public async Task BrokerHealth_RealServer_IsHealthy()
    {
        RequireSqlServer();
        var report = await BrokerHealthTestHelper.CheckAsync(b => b
            .AddSqlServerEndPoint("Sql", new SqlServerSettings { ConnectionString = ConnectionString!, TableName = _tableName },
                EndpointMode.WriteOnly));

        Assert.Equal(BrokerHealthStatus.Healthy, report.Status);
        Assert.Equal("Sql", Assert.Single(report.Endpoints).Name);
    }

    // --- helpers ---

    private static void RequireSqlServer()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString),
            $"Set {ConnectionStringVariable} to run SQL Server integration tests.");

    private SqlServerEndPoint CreateEndPoint(CapturingLogger<SqlServerEndPoint>? logger = null, int maxRetryCount = 5, int batchSize = 10)
    {
        var ep = new SqlServerEndPoint("SqlTest", new SqlServerSettings
        {
            ConnectionString = ConnectionString!,
            TableName        = _tableName,
            PollInterval     = TimeSpan.FromMilliseconds(20),
            MaxRetryCount    = maxRetryCount,
            BatchSize        = batchSize
        }, logger ?? new CapturingLogger<SqlServerEndPoint>());
        _endpoints.Add(ep);
        return ep;
    }

    private async Task<int> CountAsync(string where)
    {
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {_tableName} WHERE {where}";
        return (int)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    private static async Task<T> ScalarAsync<T>(string sql)
    {
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return (T)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    private static async Task ExecuteAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Condition was not met in time.");
            await Task.Delay(25, TestContext.Current.CancellationToken);
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

internal sealed class CapturingLogger<T> : ILogger<T>
{
    public ConcurrentQueue<(LogLevel Level, string Message, Exception? Exception)> Entries { get; } = new();

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        => Entries.Enqueue((logLevel, formatter(state, exception), exception));
}
