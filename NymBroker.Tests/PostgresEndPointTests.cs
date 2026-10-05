using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Logging;
using Npgsql;
using NymBroker.Postgres;

namespace NymBroker.Tests;

/// <summary>
/// Behaviour tests for <see cref="PostgresEndPoint"/>. The health-check test always runs; the rest need a real
/// PostgreSQL and are skipped unless <c>NYMBROKER_POSTGRES_CS</c> is set, e.g. after <c>scripts/setup-postgres.ps1</c>:
/// <c>Host=localhost;Database=nymbroker;Username=postgres;Password=postgres</c>
/// </summary>
public sealed class PostgresEndPointTests : IAsyncLifetime
{
    private const string ConnectionStringVariable = "NYMBROKER_POSTGRES_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);

    // Unique per test instance so tests can run in parallel against one database.
    private readonly string _tableName = $"nb_test_{Guid.NewGuid():N}";
    private readonly List<PostgresEndPoint> _endpoints = [];

    // --- Health check (always runs) ---

    [Fact]
    public async Task HealthCheck_UnreachableServer_ReturnsUnhealthyWithoutThrowing()
    {
        var logger = new CapturingLogger<PostgresEndPoint>();
        await using var ep = new PostgresEndPoint("Unreachable",
            new PostgresSettings { ConnectionString = "Host=127.0.0.1;Port=1;Database=nymbroker;Username=x;Password=x;Timeout=1" },
            logger);

        var result = ep.HealthCheck();

        Assert.False(result.IsHealthy);
        Assert.False(string.IsNullOrEmpty(result.Message));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error);
    }

    // --- Round trip ---

    [Fact]
    public async Task PostedMessage_ReachesHandler_WithBytesUnchanged_AndRowIsCompleted()
    {
        RequirePostgres();
        var ep = CreateEndPoint();
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var payload = Encoding.UTF8.GetBytes("""{"id":"1","message":{"text":"åäö"}}""");

        await ep.PostAsync(payload, TestContext.Current.CancellationToken);
        await ep.StartListeningAsync((raw, _) => { received.TrySetResult(raw); return Task.CompletedTask; },
            TestContext.Current.CancellationToken);

        Assert.Equal(payload, await received.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken));
        await WaitUntilAsync(async () => await CountAsync("status = 2") == 1);
    }

    // --- Handler failure: retried, then Failed; other messages still processed ---

    [Fact]
    public async Task ThrowingHandler_ReturnsToPending_ThenFailed_AndNextMessageIsStillProcessed()
    {
        RequirePostgres();
        var logger = new CapturingLogger<PostgresEndPoint>();
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
            return Task.CompletedTask;
        }, TestContext.Current.CancellationToken);

        await ok.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);
        await WaitUntilAsync(async () => await CountAsync("status = 3 AND attempt_count = 2 AND last_error = 'boom failed'") == 1);

        Assert.Equal(2, Volatile.Read(ref boomAttempts));
        Assert.Equal(1, await CountAsync("status = 2"));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error && e.Message.Contains("Unhandled error dispatching"));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("marked Failed after 2 attempts"));
    }

    // --- Poll error is logged and the loop recovers ---

    [Fact]
    public async Task PollError_IsLogged_AndLoopRecovers()
    {
        RequirePostgres();
        var logger = new CapturingLogger<PostgresEndPoint>();
        var ep = CreateEndPoint(logger, useNotifications: false);
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        Assert.True(ep.HealthCheck().IsHealthy);   // also creates the table
        await ep.StartListeningAsync((raw, _) => { received.TrySetResult(Encoding.UTF8.GetString(raw)); return Task.CompletedTask; },
            TestContext.Current.CancellationToken);

        await ExecuteAsync($"DROP TABLE {_tableName}");
        await WaitUntilAsync(() => Task.FromResult(logger.Entries.Any(e => e.Level == LogLevel.Error && e.Message.Contains("Poll error"))));

        // A fresh endpoint on the same table recreates the schema; the original loop must pick the row up.
        await CreateEndPoint().PostAsync(Encoding.UTF8.GetBytes("after-recovery"), TestContext.Current.CancellationToken);

        Assert.Equal("after-recovery", await received.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken));
        Assert.DoesNotContain(logger.Entries, e => e.Level == LogLevel.Critical);
    }

    // --- No handler calls after StopListeningAsync returns ---

    [Fact]
    public async Task AfterStopListening_HandlerIsNeverCalled_AndRowStaysPending()
    {
        RequirePostgres();
        var ep = CreateEndPoint();
        var calls = 0;
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.StartListeningAsync((_, _) => { Interlocked.Increment(ref calls); first.TrySetResult(); return Task.CompletedTask; },
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
        RequirePostgres();
        var ep = CreateEndPoint();
        var fastHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSlow = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.PostAsync(Encoding.UTF8.GetBytes("fast"), TestContext.Current.CancellationToken);
        await ep.PostAsync(Encoding.UTF8.GetBytes("slow"), TestContext.Current.CancellationToken);
        await ep.StartListeningAsync(async (raw, _) =>
        {
            if (Encoding.UTF8.GetString(raw) == "fast") { fastHandled.TrySetResult(); return; }
            await releaseSlow.Task;   // still running when stop is requested; completes normally
        }, TestContext.Current.CancellationToken);

        await fastHandled.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);
        var stop = ep.StopListeningAsync();   // cancels the loop, then waits for it
        releaseSlow.SetResult();
        await stop;

        Assert.Equal(2, await CountAsync("status = 2"));
        Assert.Equal(0, await CountAsync("status = 1"));
    }

    // --- A late finalize (stale attempt) must not overwrite a re-claimed row ---

    [Fact]
    public async Task Finalize_WithStaleAttempt_DoesNotOverwriteReclaimedRow()
    {
        RequirePostgres();
        var ep = CreateEndPoint();
        await ep.PostAsync(Encoding.UTF8.GetBytes("x"), TestContext.Current.CancellationToken);
        // Simulate: claimed twice (lease expired once), the current owner holds attempt 2.
        await ExecuteAsync($"UPDATE {_tableName} SET status = 1, attempt_count = 2, locked_until_utc = NOW() + INTERVAL '5 minutes'");
        var queueId = await ScalarAsync<long>($"SELECT queue_id FROM {_tableName}");

        // The first owner (attempt 1) finishes late and tries to mark it Completed.
        await using (var conn = new NpgsqlConnection(ConnectionString))
        {
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            await using var cmd = new NpgsqlCommand(PostgresQueueSql.FinalizeMessages(_tableName), conn);
            cmd.Parameters.AddWithValue("queueIds", new[] { queueId });
            cmd.Parameters.AddWithValue("attempts", new[] { 1 });
            cmd.Parameters.AddWithValue("statuses", new[] { 2 });
            cmd.Parameters.AddWithValue("errors", new string?[] { null });
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        Assert.Equal(1, await CountAsync("status = 1 AND attempt_count = 2"));
    }

    // --- Backlog drains without waiting PollInterval between batches (0.1.x waited after every batch) ---

    [Fact]
    public async Task Backlog_DrainsBackToBack_NotOneBatchPerPollInterval()
    {
        RequirePostgres();
        const int messageCount = 50;
        var ep = CreateEndPoint(batchSize: 5, pollInterval: TimeSpan.FromSeconds(1));   // UseNotifications on (default)
        for (var i = 0; i < messageCount; i++)
            await ep.PostAsync(Encoding.UTF8.GetBytes($"m{i}"), TestContext.Current.CancellationToken);

        var handled = 0;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sw = Stopwatch.StartNew();
        await ep.StartListeningAsync((_, _) =>
        {
            if (Interlocked.Increment(ref handled) == messageCount) done.TrySetResult();
            return Task.CompletedTask;
        }, TestContext.Current.CancellationToken);
        await done.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        // 10 batches × 1 s poll interval would take ≥ 9 s if the loop waited between full batches.
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5), $"drain took {sw.Elapsed}");
    }

    // --- NOTIFY wakes an idle listener long before PollInterval ---

    [Fact]
    public async Task Notify_WakesIdleListener_BeforePollInterval()
    {
        RequirePostgres();
        var ep = CreateEndPoint(pollInterval: TimeSpan.FromSeconds(30));
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await ep.StartListeningAsync((_, _) => { received.TrySetResult(); return Task.CompletedTask; },
            TestContext.Current.CancellationToken);
        await Task.Delay(500, TestContext.Current.CancellationToken);   // let the loop go idle and LISTEN

        var sw = Stopwatch.StartNew();
        await ep.PostAsync(Encoding.UTF8.GetBytes("wake"), TestContext.Current.CancellationToken);
        await received.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5), $"wake-up took {sw.Elapsed}");
    }

    // --- Tables created by 0.1.x get the partial index and lose the old full status indexes ---

    [Fact]
    public async Task ExistingTableWithLegacyIndexes_IsMigratedToPartialIndex()
    {
        RequirePostgres();
        await ExecuteAsync($"""
            CREATE TABLE {_tableName} (
                queue_id BIGSERIAL PRIMARY KEY, message_id UUID NOT NULL UNIQUE,
                status INTEGER NOT NULL DEFAULT 0 CHECK (status IN (0, 1, 2, 3)),
                created_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(), locked_until_utc TIMESTAMPTZ NULL,
                completed_at_utc TIMESTAMPTZ NULL, failed_at_utc TIMESTAMPTZ NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0, last_error TEXT NULL, payload BYTEA NOT NULL);
            CREATE INDEX ix_{_tableName}_status_created ON {_tableName}(status, created_at_utc, queue_id);
            CREATE INDEX ix_{_tableName}_status_locked_until ON {_tableName}(status, locked_until_utc);
            """);

        Assert.True(CreateEndPoint().HealthCheck().IsHealthy);   // runs the schema step

        var indexes = await ScalarAsync<string>($"SELECT string_agg(indexname, ',' ORDER BY indexname) FROM pg_indexes WHERE tablename = '{_tableName}'");
        Assert.Contains($"ix_{_tableName}_active", indexes);
        Assert.DoesNotContain("status_created", indexes);
        Assert.DoesNotContain("status_locked_until", indexes);
    }

    // --- Competing consumers: SKIP LOCKED prevents double-processing ---

    [Fact]
    public async Task TwoEndpointsOnOneTable_ProcessEachMessageExactlyOnce()
    {
        RequirePostgres();
        const int messageCount = 60;
        var seen = new ConcurrentDictionary<string, int>();
        var producer = CreateEndPoint();
        for (var i = 0; i < messageCount; i++)
            await producer.PostAsync(Encoding.UTF8.GetBytes($"m{i}"), TestContext.Current.CancellationToken);

        Task Handler(byte[] raw, CancellationToken _)
        {
            seen.AddOrUpdate(Encoding.UTF8.GetString(raw), 1, (_, n) => n + 1);
            return Task.CompletedTask;
        }

        await CreateEndPoint(batchSize: 5).StartListeningAsync(Handler, TestContext.Current.CancellationToken);
        await CreateEndPoint(batchSize: 5).StartListeningAsync(Handler, TestContext.Current.CancellationToken);

        await WaitUntilAsync(async () => await CountAsync("status = 2") == messageCount);
        Assert.Equal(messageCount, seen.Count);
        Assert.All(seen.Values, n => Assert.Equal(1, n));
    }

    // --- helpers ---

    private static void RequirePostgres()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString),
            $"Set {ConnectionStringVariable} to run PostgreSQL integration tests.");

    private PostgresEndPoint CreateEndPoint(CapturingLogger<PostgresEndPoint>? logger = null, int maxRetryCount = 5, int batchSize = 10,
        TimeSpan? pollInterval = null, bool useNotifications = true)
    {
        var ep = new PostgresEndPoint("PgTest", new PostgresSettings
        {
            ConnectionString = ConnectionString!,
            TableName        = _tableName,
            PollInterval     = pollInterval ?? TimeSpan.FromMilliseconds(20),
            MaxRetryCount    = maxRetryCount,
            BatchSize        = batchSize,
            UseNotifications = useNotifications
        }, logger ?? new CapturingLogger<PostgresEndPoint>());
        _endpoints.Add(ep);
        return ep;
    }

    private async Task<long> CountAsync(string where)
        => await ScalarAsync<long>($"SELECT COUNT(*) FROM {_tableName} WHERE {where}");

    private static async Task<T> ScalarAsync<T>(string sql)
    {
        await using var conn = new NpgsqlConnection(ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
        return (T)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    private static async Task ExecuteAsync(string sql)
    {
        await using var conn = new NpgsqlConnection(ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = new NpgsqlCommand(sql, conn);
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
            await ExecuteAsync($"DROP TABLE IF EXISTS {_tableName}");
    }
}
