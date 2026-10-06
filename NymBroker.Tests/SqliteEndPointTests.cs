using System.Text;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Sql;
using NymBroker.Core.Endpoint;

namespace NymBroker.Tests;

public sealed class SqliteEndPointTests : IAsyncDisposable
{
    private readonly SqliteEndPoint _ep = new("test-sql",
        new SqliteSettings { ConnectionString = "Data Source=:memory:", AutoCreateTable = true },
        NullLogger<SqliteEndPoint>.Instance);

    public async ValueTask DisposeAsync() => await _ep.DisposeAsync();

    [Fact]
    public async Task PostAsync_InsertsRow()
    {
        var payload = """{"test":true}""";
        await _ep.PostAsync(Encoding.UTF8.GetBytes(payload), TestContext.Current.CancellationToken);

        var items = new List<string>();
        await foreach (var item in _ep.ReadAsync(TestContext.Current.CancellationToken))
            items.Add(item);

        Assert.Single(items);
        Assert.Equal(payload, items[0]);
    }

    [Fact]
    public async Task ReadAsync_YieldsPendingRows()
    {
        await Post("""{"n":1}""");
        await Post("""{"n":2}""");
        await Post("""{"n":3}""");

        var items = new List<string>();
        await foreach (var item in _ep.ReadAsync(TestContext.Current.CancellationToken))
            items.Add(item);

        Assert.Equal(3, items.Count);
    }

    [Fact]
    public async Task ReadAsync_MarksRowsProcessed()
    {
        await Post("""{"x":1}""");

        await foreach (var _ in _ep.ReadAsync(TestContext.Current.CancellationToken)) { }

        var items = new List<string>();
        await foreach (var item in _ep.ReadAsync(TestContext.Current.CancellationToken))
            items.Add(item);

        Assert.Empty(items);
    }

    [Fact]
    public async Task ReadAsync_IgnoresAlreadyProcessed()
    {
        await Post("""{"a":1}""");
        await Post("""{"a":2}""");

        await foreach (var _ in _ep.ReadAsync(TestContext.Current.CancellationToken)) { }

        await Post("""{"a":3}""");

        var items = new List<string>();
        await foreach (var item in _ep.ReadAsync(TestContext.Current.CancellationToken))
            items.Add(item);

        Assert.Single(items);
    }

    [Fact]
    public async Task ReadAsync_RespectsBatchSize()
    {
        await using var ep = new SqliteEndPoint("batch-test",
            new SqliteSettings { ConnectionString = "Data Source=:memory:", AutoCreateTable = true, BatchSize = 2 },
            NullLogger<SqliteEndPoint>.Instance);

        for (var i = 0; i < 5; i++)
            await ep.PostAsync(Encoding.UTF8.GetBytes($$$"""{"i":{{{i}}}}"""), TestContext.Current.CancellationToken);

        var items = new List<string>();
        await foreach (var item in ep.ReadAsync(TestContext.Current.CancellationToken))
            items.Add(item);

        Assert.Equal(2, items.Count);
    }

    [Fact]
    public async Task ReadAsync_NoDuplicatesOnConsecutiveCalls()
    {
        await Post("""{"seq":1}""");
        await Post("""{"seq":2}""");

        var first = new List<string>();
        await foreach (var item in _ep.ReadAsync(TestContext.Current.CancellationToken))
            first.Add(item);

        var second = new List<string>();
        await foreach (var item in _ep.ReadAsync(TestContext.Current.CancellationToken))
            second.Add(item);

        Assert.Equal(2, first.Count);
        Assert.Empty(second);
    }

    [Fact]
    public void HealthCheck_ReturnsHealthy()
    {
        var result = _ep.HealthCheck();
        Assert.True(result.IsHealthy);
    }

    [Fact]
    public async Task StartListeningAsync_DeliversMessages()
    {
        await using var ep = new SqliteEndPoint("listen-test",
            new SqliteSettings
            {
                ConnectionString = "Data Source=:memory:",
                AutoCreateTable  = true,
                PollInterval     = TimeSpan.Zero,
                MaxRetryCount    = 3
            },
            NullLogger<SqliteEndPoint>.Instance);

        await ep.PostAsync(Encoding.UTF8.GetBytes("""{"e":1}"""), TestContext.Current.CancellationToken);
        await ep.PostAsync(Encoding.UTF8.GetBytes("""{"e":2}"""), TestContext.Current.CancellationToken);

        var received = new List<string>();
        var tcs = new TaskCompletionSource();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await ep.StartListeningAsync(async (msg, _) =>
        {
            received.Add(System.Text.Encoding.UTF8.GetString(msg));
            if (received.Count >= 2) tcs.TrySetResult();
            await Task.CompletedTask;
            return ProcessResult.Completed;
        }, cts.Token);

        await tcs.Task;
        await ep.StopListeningAsync();

        Assert.Equal(2, received.Count);
    }

    [Fact]
    public async Task StartListeningAsync_RetriesFailedMessages_AndEventuallyCompletes()
    {
        await using var ep = new SqliteEndPoint("retry-test",
            new SqliteSettings
            {
                ConnectionString = "Data Source=:memory:",
                AutoCreateTable  = true,
                PollInterval     = TimeSpan.Zero,
                MaxRetryCount    = 3,
                LeaseTimeout     = TimeSpan.FromSeconds(1)
            },
            NullLogger<SqliteEndPoint>.Instance);

        await ep.PostAsync(Encoding.UTF8.GetBytes("""{"retry":true}"""), TestContext.Current.CancellationToken);

        var attempts = 0;
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await ep.StartListeningAsync((_, _) =>
        {
            attempts++;
            if (attempts < 2)
                throw new InvalidOperationException("Transient failure");

            completed.TrySetResult();
            return Task.FromResult(ProcessResult.Completed);
        }, cts.Token);

        await completed.Task;
        await ep.StopListeningAsync();

        Assert.Equal(2, attempts);

        var items = new List<string>();
        await foreach (var item in ep.ReadAsync(TestContext.Current.CancellationToken))
            items.Add(item);

        Assert.Empty(items);
    }

    [Fact]
    public async Task StartListeningAsync_MarksMessageFailed_AfterMaxRetries()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"nymbroker-sqlite-{Guid.NewGuid():N}.db");
        SqliteEndPoint? ep = null;
        try
        {
            var connectionString = $"Data Source={dbPath}";
            ep = new SqliteEndPoint("failed-test",
                new SqliteSettings
                {
                    ConnectionString = connectionString,
                    AutoCreateTable  = true,
                    PollInterval     = TimeSpan.Zero,
                    MaxRetryCount    = 2,
                    LeaseTimeout     = TimeSpan.FromSeconds(1)
                },
                NullLogger<SqliteEndPoint>.Instance);

            await ep.PostAsync(Encoding.UTF8.GetBytes("""{"fail":true}"""), TestContext.Current.CancellationToken);

            var attempts = 0;
            var exhausted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await ep.StartListeningAsync((_, _) =>
            {
                attempts++;
                if (attempts >= 2) exhausted.TrySetResult();
                throw new InvalidOperationException("Permanent failure");
            }, cts.Token);

            await exhausted.Task;
            await ep.StopListeningAsync();

            await using var conn = new SqliteConnection(connectionString);
            await conn.OpenAsync(TestContext.Current.CancellationToken);

            var failedCount = await SqliteTestDb.ScalarAsync<int>(conn, "SELECT COUNT(1) FROM NymBrokerMessages WHERE Status = 3");
            var pendingCount = await SqliteTestDb.ScalarAsync<int>(conn, "SELECT COUNT(1) FROM NymBrokerMessages WHERE Status = 0");
            var inProgressCount = await SqliteTestDb.ScalarAsync<int>(conn, "SELECT COUNT(1) FROM NymBrokerMessages WHERE Status = 1");

            Assert.Equal(2, attempts);
            Assert.Equal(1, failedCount);
            Assert.Equal(0, pendingCount);
            Assert.Equal(0, inProgressCount);
        }
        finally
        {
            if (ep is not null)
                await ep.DisposeAsync();

            if (File.Exists(dbPath))
            {
                try
                {
                    File.Delete(dbPath);
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
        }
    }

    // --- Native rewrite (#60) ---

    private static SqliteEndPoint FileEndPoint(string connectionString, int batchSize = 10, TimeSpan? pollInterval = null,
        int maxRetryCount = 5) => new("file-test", new SqliteSettings
    {
        ConnectionString = connectionString,
        AutoCreateTable  = true,
        BatchSize        = batchSize,
        PollInterval     = pollInterval ?? TimeSpan.FromMilliseconds(10),
        MaxRetryCount    = maxRetryCount
    }, NullLogger<SqliteEndPoint>.Instance);

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("Condition was not met in time.");
            await Task.Delay(20, TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task BinaryPayload_NotValidUtf8_RoundTripsUnchanged()
    {
        await using var ep = new SqliteEndPoint("binary", new SqliteSettings
        {
            ConnectionString = "Data Source=:memory:", PollInterval = TimeSpan.FromMilliseconds(10)
        }, NullLogger<SqliteEndPoint>.Instance);
        var payload = new byte[] { 0xFF, 0xFE, 0x00, 0x80 };
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.PostAsync(payload, TestContext.Current.CancellationToken);
        await ep.StartListeningAsync((raw, _) => { received.TrySetResult(raw); return Task.FromResult(ProcessResult.Completed); },
            TestContext.Current.CancellationToken);

        Assert.Equal(payload, await received.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ExistingV1Table_IsMigrated_AndPendingRowsAreProcessed()
    {
        using var db = new SqliteTestDb.TempFile();
        await using (var conn = await SqliteTestDb.OpenAsync(db.ConnectionString))
        {
            // The schema created by versions before #60.
            await SqliteTestDb.ExecuteAsync(conn, """
                CREATE TABLE NymBrokerMessages (
                    QueueId INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, MessageId TEXT NOT NULL UNIQUE,
                    Status INTEGER NOT NULL DEFAULT 0 CHECK (Status IN (0, 1, 2, 3)), CreatedAtUtc INTEGER NOT NULL DEFAULT (unixepoch()),
                    LockedUntilUtc INTEGER NULL, CompletedAtUtc INTEGER NULL, FailedAtUtc INTEGER NULL,
                    AttemptCount INTEGER NOT NULL DEFAULT 0, LastError TEXT NULL, Payload TEXT NOT NULL);
                CREATE INDEX IX_NymBrokerMessages_Status_CreatedAt ON NymBrokerMessages(Status, CreatedAtUtc, QueueId);
                CREATE INDEX IX_NymBrokerMessages_LockedUntil ON NymBrokerMessages(Status, LockedUntilUtc);
                INSERT INTO NymBrokerMessages (MessageId, Status, Payload) VALUES ('a', 0, '{"n":1}'), ('b', 0, '{"n":2}'), ('c', 2, '{"n":3}');
                """);
        }

        await using var ep = FileEndPoint(db.ConnectionString);
        var received = new System.Collections.Concurrent.ConcurrentQueue<string>();
        await ep.StartListeningAsync((raw, _) => { received.Enqueue(Encoding.UTF8.GetString(raw)); return Task.FromResult(ProcessResult.Completed); },
            TestContext.Current.CancellationToken);
        await WaitUntilAsync(() => Task.FromResult(received.Count >= 2));
        await ep.StopListeningAsync();

        Assert.Equal(["""{"n":1}""", """{"n":2}"""], received.ToArray());
        Assert.Equal(2, await SqliteTestDb.ScalarAsync<int>(db.ConnectionString, "PRAGMA user_version"));
        Assert.Equal("BLOB", await SqliteTestDb.ScalarAsync<string>(db.ConnectionString,
            "SELECT type FROM pragma_table_info('NymBrokerMessages') WHERE name = 'Payload'"));
        Assert.Equal(3, await SqliteTestDb.ScalarAsync<int>(db.ConnectionString, "SELECT COUNT(1) FROM NymBrokerMessages WHERE Status = 2"));
    }

    [Fact]
    public async Task BatchFinalize_MixedOutcomes_AreWrittenTogether()
    {
        using var db = new SqliteTestDb.TempFile();
        await using var ep = FileEndPoint(db.ConnectionString, batchSize: 3);
        await ep.PostBatchAsync(["completed"u8.ToArray(), "retry"u8.ToArray(), "deadletter"u8.ToArray()], TestContext.Current.CancellationToken);
        using var listening = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var calls = 0;

        await ep.StartListeningAsync((raw, _) =>
        {
            // Stop after the first batch so its outcomes are written on shutdown, before anything is claimed again.
            if (Interlocked.Increment(ref calls) == 3) listening.Cancel();
            return Task.FromResult(Encoding.UTF8.GetString(raw) switch
            {
                "completed" => ProcessResult.Completed,
                "retry" => ProcessResult.Retry(description: "later"),
                _ => ProcessResult.DeadLetter(DeadLetterReasons.Expired, "too old")
            });
        }, listening.Token);
        await WaitUntilAsync(() => Task.FromResult(Volatile.Read(ref calls) >= 3));
        await ep.StopListeningAsync();

        var rows = await SqliteTestDb.QueryAsync(db.ConnectionString, "SELECT Status, LastError FROM NymBrokerMessages ORDER BY QueueId");
        Assert.Equal([2L, 0L, 3L], rows.Select(r => (long)r[0]!));
        Assert.Equal([null, "later", "Expired: too old"], rows.Select(r => (string?)r[1]));
    }

    [Fact]
    public async Task StaleAttempt_DoesNotOverwriteReclaimedRow()
    {
        using var db = new SqliteTestDb.TempFile();
        await using var ep = FileEndPoint(db.ConnectionString);
        await ep.PostAsync("x"u8.ToArray(), TestContext.Current.CancellationToken);

        await foreach (var _ in ep.ReadAsync(TestContext.Current.CancellationToken))
        {
            // Claimed with attempt 1. Simulate another poller re-claiming it after our lease expired (attempt 2).
            await SqliteTestDb.ExecuteAsync(db.ConnectionString, "UPDATE NymBrokerMessages SET AttemptCount = 2");
        }   // moving past the item completes it — with the stale attempt 1

        var row = Assert.Single(await SqliteTestDb.QueryAsync(db.ConnectionString, "SELECT Status, AttemptCount FROM NymBrokerMessages"));
        Assert.Equal(1L, row[0]);   // still InProgress for the other poller
        Assert.Equal(2L, row[1]);
    }

    [Fact]
    public async Task Backlog_DrainsBackToBack_WithoutWaitingPollInterval()
    {
        await using var ep = new SqliteEndPoint("drain", new SqliteSettings
        {
            ConnectionString = "Data Source=:memory:", BatchSize = 10, PollInterval = TimeSpan.FromSeconds(1)
        }, NullLogger<SqliteEndPoint>.Instance);
        await ep.PostBatchAsync(Enumerable.Range(0, 50).Select(i => Encoding.UTF8.GetBytes($"m{i}")).ToList(), TestContext.Current.CancellationToken);
        var handled = 0;
        var all = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sw = System.Diagnostics.Stopwatch.StartNew();

        await ep.StartListeningAsync((_, _) =>
        {
            if (Interlocked.Increment(ref handled) == 50) all.TrySetResult();
            return Task.FromResult(ProcessResult.Completed);
        }, TestContext.Current.CancellationToken);
        await all.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        sw.Stop();

        // Five full batches; waiting PollInterval between them would take at least 4 s.
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(3), $"Took {sw.Elapsed}.");
    }

    [Fact]
    public async Task FileDatabase_UsesWal_AndTwoEndpointsProcessEachMessageOnce()
    {
        using var db = new SqliteTestDb.TempFile();
        await using var ep1 = FileEndPoint(db.ConnectionString);
        await using var ep2 = FileEndPoint(db.ConnectionString);
        await ep1.PostBatchAsync(Enumerable.Range(0, 100).Select(i => Encoding.UTF8.GetBytes($"m{i}")).ToList(), TestContext.Current.CancellationToken);
        var counts = new System.Collections.Concurrent.ConcurrentDictionary<string, int>();

        Task<ProcessResult> Handle(byte[] raw, CancellationToken _)
        {
            counts.AddOrUpdate(Encoding.UTF8.GetString(raw), 1, (_, n) => n + 1);
            return Task.FromResult(ProcessResult.Completed);
        }
        await ep1.StartListeningAsync(Handle, TestContext.Current.CancellationToken);
        await ep2.StartListeningAsync(Handle, TestContext.Current.CancellationToken);
        await WaitUntilAsync(async () => await SqliteTestDb.ScalarAsync<int>(db.ConnectionString,
            "SELECT COUNT(1) FROM NymBrokerMessages WHERE Status = 2") == 100);
        await ep1.StopListeningAsync();
        await ep2.StopListeningAsync();

        Assert.Equal(100, counts.Count);
        Assert.All(counts.Values, n => Assert.Equal(1, n));
        Assert.Equal("wal", await SqliteTestDb.ScalarAsync<string>(db.ConnectionString, "PRAGMA journal_mode"));
    }

    [Fact]
    public void InvalidTableName_Throws()
    {
        Assert.Throws<ArgumentException>(() => new SqliteEndPoint("bad",
            new SqliteSettings { ConnectionString = "Data Source=:memory:", TableName = "x; DROP TABLE y" },
            NullLogger<SqliteEndPoint>.Instance));
    }

    [Fact]
    public async Task Claim_UsesThePartialActiveIndex()
    {
        await using var conn = await SqliteTestDb.OpenAsync("Data Source=:memory:");
        await SqliteTestDb.ExecuteAsync(conn, SqliteQueueSql.CreateTable("Q"));
        await using var command = conn.CreateCommand();
        command.CommandText = "EXPLAIN QUERY PLAN " + SqliteQueueSql.Claim("Q");
        command.Parameters.AddWithValue("$leaseSeconds", 60);
        command.Parameters.AddWithValue("$batchSize", 10);
        var plan = new List<string>();
        await using (var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken))
            while (await reader.ReadAsync(TestContext.Current.CancellationToken))
                plan.Add(reader.GetString(3));

        Assert.Contains(plan, line => line.Contains("IX_Q_Active"));
    }

    private Task Post(string payload) =>
        _ep.PostAsync(Encoding.UTF8.GetBytes(payload));
}
