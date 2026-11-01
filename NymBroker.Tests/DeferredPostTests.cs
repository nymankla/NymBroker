using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.File;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;
using NymBroker.Core.Transform;
using NymBroker.Endpoint.Sqlite;

namespace NymBroker.Tests;

/// <summary>Checks every transport's integration test runs through <see cref="IDeferredEndPoint"/>.</summary>
internal static class DeferredContract
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>A deferred message waits until it is due, and a message posted after it does not wait for it.</summary>
    public static async Task DeferredMessageWaits_OtherMessagesDoNot<TEndPoint>(TEndPoint endpoint)
        where TEndPoint : IDeferredEndPoint, IEndPointEventDriven
    {
        var deliveries = new ConcurrentQueue<(string Body, DateTimeOffset At)>();
        var deliverAt = DateTimeOffset.UtcNow.AddSeconds(3);

        await endpoint.PostDeferredAsync("""{"n":"later"}"""u8.ToArray(), deliverAt, Ct);
        await endpoint.PostAsync("""{"n":"now"}"""u8.ToArray(), Ct);
        await endpoint.StartListeningAsync((raw, _) =>
        {
            deliveries.Enqueue((Encoding.UTF8.GetString(raw), DateTimeOffset.UtcNow));
            return Task.FromResult(ProcessResult.Completed);
        }, Ct);

        await WaitUntilAsync(() => deliveries.Count >= 2, TimeSpan.FromSeconds(30));
        var received = deliveries.ToArray();
        Assert.Equal(2, received.Length);
        Assert.Contains("now", received[0].Body);
        Assert.Contains("later", received[1].Body);
        Assert.True(received[0].At < deliverAt, "The other message waited for the deferred one.");
        // A database or server compares with its own clock, which may differ a little from this one.
        Assert.True(received[1].At >= deliverAt.AddSeconds(-1), $"Delivered at {received[1].At:O}, due at {deliverAt:O}.");
    }

    public static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("Condition not met in time.");
            await Task.Delay(20, Ct);
        }
    }
}

public sealed class DeferredPostTests : IAsyncDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private readonly ServiceProvider _services = new ServiceCollection().BuildServiceProvider();
    private readonly string _folder = Path.Combine(Path.GetTempPath(), $"nymbroker-deferred-{Guid.NewGuid():N}");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public sealed class Job { public int Id { get; set; } }

    private sealed class PlainEndPoint : IEndPoint
    {
        public Task PostAsync(byte[] message, CancellationToken ct = default) => Task.CompletedTask;
        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
    }

    private sealed class RecordingEndPoint(EndpointMode mode = EndpointMode.ReadWrite) : IDeferredEndPoint
    {
        public EndpointMode Mode => mode;
        public List<(byte[] Message, DateTimeOffset DeliverAt)> Deferred { get; } = [];
        public int Posted { get; private set; }

        public Task PostAsync(byte[] message, CancellationToken ct = default)
        {
            Posted++;
            return Task.CompletedTask;
        }

        public Task PostDeferredAsync(byte[] message, DateTimeOffset deliverAt, CancellationToken ct = default)
        {
            Deferred.Add((message, deliverAt));
            return Task.CompletedTask;
        }

        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
    }

    private sealed class FixedTransformer(byte[]? output) : IOutputTransformer
    {
        public byte[]? Transform(RawMessageContext message, string destinationEndpoint) => output;
    }

    public async ValueTask DisposeAsync()
    {
        await _services.DisposeAsync();
        if (Directory.Exists(_folder)) Directory.Delete(_folder, recursive: true);
    }

    private NymBrokerImpl CreateBroker(string name, IEndPoint endpoint)
    {
        var broker = TestBroker.Create(_services);
        broker.AddEndpoint(name, endpoint);
        return broker;
    }

    // --- Broker ---

    [Fact]
    public async Task EndpointWithoutSupport_Throws_EvenForATimeInThePast()
    {
        var broker = CreateBroker("Plain", new PlainEndPoint());

        var future = await Assert.ThrowsAsync<NotSupportedException>(() =>
            broker.PostDeferredAsync("Plain", new Job(), DateTimeOffset.UtcNow.AddMinutes(1), Ct));
        await Assert.ThrowsAsync<NotSupportedException>(() =>
            broker.PostDeferredAsync("Plain", new Job(), DateTimeOffset.UtcNow.AddMinutes(-1), Ct));
        await Assert.ThrowsAsync<NotSupportedException>(() =>
            broker.PostRawDeferredAsync("Plain", "{}"u8.ToArray().AsMemory(), DateTimeOffset.UtcNow.AddMinutes(1), Ct));

        Assert.Contains("'Plain'", future.Message);
    }

    [Fact]
    public async Task ReadOnlyOrUnknownEndpoint_Throws()
    {
        var broker = CreateBroker("Ro", new RecordingEndPoint(EndpointMode.ReadOnly));

        await Assert.ThrowsAsync<InvalidOperationException>(() => broker.PostDeferredAsync("Ro", new Job(), DateTimeOffset.UtcNow, Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => broker.PostDeferredAsync("Missing", new Job(), DateTimeOffset.UtcNow, Ct));
    }

    [Fact]
    public async Task Envelope_IsCreatedWhenDue_AndPassedWithTheTime()
    {
        var endpoint = new RecordingEndPoint();
        var deliverAt = DateTimeOffset.UtcNow.AddHours(2);

        await CreateBroker("Q", endpoint).PostDeferredAsync("Q", new Job { Id = 7 }, deliverAt, Ct);

        var (message, passedTime) = Assert.Single(endpoint.Deferred);
        Assert.Equal(0, endpoint.Posted);
        Assert.Equal(deliverAt, passedTime);
        using var envelope = JsonDocument.Parse(message);
        Assert.Equal(deliverAt.UtcDateTime, envelope.RootElement.GetProperty("created").GetDateTime());
        Assert.Equal(7, envelope.RootElement.GetProperty("message").GetProperty("id").GetInt32());
    }

    [Fact]
    public async Task TimeInThePast_LeavesCreatedAtNow()
    {
        var endpoint = new RecordingEndPoint();
        var before = DateTime.UtcNow;

        await CreateBroker("Q", endpoint).PostDeferredAsync("Q", new Job(), DateTimeOffset.UtcNow.AddDays(-1), Ct);

        using var envelope = JsonDocument.Parse(Assert.Single(endpoint.Deferred).Message);
        Assert.True(envelope.RootElement.GetProperty("created").GetDateTime() >= before);
    }

    [Fact]
    public async Task MaxMessageAge_CountsFromTheDeliveryTime()
    {
        var target = new RecordingEndPoint();
        var deadLetters = new MemoryQueueEndPoint("Dlq");
        var broker = CreateBroker("Q", target);
        broker.AddEndpoint("Dlq", deadLetters);
        broker.SetDeadLetterEndpoint("Dlq");
        broker.SetMaxMessageAge(TimeSpan.FromMilliseconds(500));

        await broker.PostDeferredAsync("Q", new Job(), DateTimeOffset.UtcNow.AddSeconds(1), Ct);
        await Task.Delay(TimeSpan.FromSeconds(1), Ct);   // older than the maximum since it was posted, but only just due
        await broker.ProcessAsync(Assert.Single(target.Deferred).Message, "Q", Ct);

        var deadLettered = new List<string>();
        await foreach (var item in deadLetters.ReadAsync(Ct)) deadLettered.Add(item);
        Assert.Empty(deadLettered);
    }

    [Fact]
    public async Task OutputTransformer_SeesTheMessage_AndNullSendsNothing()
    {
        var endpoint = new RecordingEndPoint();
        var broker = CreateBroker("Q", endpoint);
        var later = DateTimeOffset.UtcNow.AddMinutes(1);

        broker.AddOutputTransformer(new FixedTransformer("custom"u8.ToArray()), "Q");
        await broker.PostDeferredAsync("Q", new Job(), later, Ct);
        Assert.Equal("custom"u8.ToArray(), Assert.Single(endpoint.Deferred).Message);

        broker.AddOutputTransformer(new FixedTransformer(null), "Q");
        await broker.PostDeferredAsync("Q", new Job(), later, Ct);
        Assert.Single(endpoint.Deferred);
    }

    [Fact]
    public async Task Raw_SendsTheBytesUnchanged_FromMemoryAndStream()
    {
        var endpoint = new RecordingEndPoint();
        var broker = CreateBroker("Q", endpoint);
        var later = DateTimeOffset.UtcNow.AddMinutes(1);
        byte[] bytes = [1, 2, 3];

        await broker.PostRawDeferredAsync("Q", bytes.AsMemory(), later, Ct);
        await broker.PostRawDeferredAsync("Q", new MemoryStream(bytes), later, Ct);

        Assert.All(endpoint.Deferred, d => Assert.Equal(bytes, d.Message));
        Assert.Equal(2, endpoint.Deferred.Count);
    }

    [Fact]
    public async Task BytesAsATypedMessage_AreRejected()
    {
        var broker = CreateBroker("Q", new RecordingEndPoint());
        await Assert.ThrowsAsync<ArgumentException>(() => broker.PostDeferredAsync("Q", new byte[] { 1 }, DateTimeOffset.UtcNow, Ct));
    }

    // --- Memory ---

    [Fact]
    public async Task Memory_DeferredMessageWaits_OtherMessagesDoNot()
    {
        var endpoint = new MemoryQueueEndPoint("Mem");
        await DeferredContract.DeferredMessageWaits_OtherMessagesDoNot(endpoint);
        await endpoint.StopListeningAsync();
    }

    [Fact]
    public async Task Memory_TimeInThePast_IsQueuedAtOnce()
    {
        var endpoint = new MemoryQueueEndPoint("Mem");

        await endpoint.PostDeferredAsync("{}"u8.ToArray(), DateTimeOffset.UtcNow.AddMinutes(-1), Ct);

        var items = new List<string>();
        await foreach (var item in endpoint.ReadAsync(Ct)) items.Add(item);
        Assert.Single(items);
    }

    [Fact]
    public async Task Memory_Waiting_DoesNotUseCapacity()
    {
        var endpoint = new MemoryQueueEndPoint("Mem", capacity: 1);

        // Would block on the second message if the waiting ones were in the bounded channel.
        await endpoint.PostDeferredAsync("{}"u8.ToArray(), DateTimeOffset.UtcNow.AddMinutes(1), Ct);
        await endpoint.PostDeferredAsync("{}"u8.ToArray(), DateTimeOffset.UtcNow.AddMinutes(1), Ct).WaitAsync(Timeout, Ct);
        await endpoint.PostAsync("{}"u8.ToArray(), Ct).WaitAsync(Timeout, Ct);

        await endpoint.StopListeningAsync();
    }

    [Fact]
    public async Task Memory_StopWhileWaiting_DropsWithAWarning_AndLaterPostsThrow()
    {
        var logger = new CapturingLogger<MemoryQueueEndPoint>();
        var endpoint = new MemoryQueueEndPoint("Mem", logger: logger);
        await endpoint.PostDeferredAsync("{}"u8.ToArray(), DateTimeOffset.UtcNow.AddHours(1), Ct);

        await endpoint.StopListeningAsync();

        await DeferredContract.WaitUntilAsync(() => logger.Entries.Any(e => e.Level == LogLevel.Warning && e.Message.Contains("dropped")), Timeout);
        await Assert.ThrowsAnyAsync<Exception>(() => endpoint.PostDeferredAsync("{}"u8.ToArray(), DateTimeOffset.UtcNow.AddHours(1), Ct));
    }

    // --- File ---

    private FileEndPoint CreateFileEndPoint(TimeSpan? pollInterval = null) => new("File", new FileSettings
    {
        ReadPath = _folder,
        PostPath = _folder,
        IsAbsolutePath = true,
        PollInterval = pollInterval ?? TimeSpan.Zero
    }, NullLogger<FileEndPoint>.Instance);

    [Fact]
    public async Task File_DeferredMessageWaits_OtherMessagesDoNot_WithoutPolling()
    {
        var endpoint = CreateFileEndPoint();
        await DeferredContract.DeferredMessageWaits_OtherMessagesDoNot(endpoint);
        await endpoint.StopListeningAsync();

        Assert.Equal(2, Directory.GetFiles(_folder, "*.processed").Length);
    }

    [Fact]
    public async Task File_ReadAsync_SkipsAFileThatIsNotDue()
    {
        var endpoint = CreateFileEndPoint();
        await endpoint.PostDeferredAsync("{}"u8.ToArray(), DateTimeOffset.UtcNow.AddMinutes(1), Ct);

        var items = new List<string>();
        await foreach (var item in endpoint.ReadAsync(Ct)) items.Add(item);

        Assert.Empty(items);
        Assert.Single(Directory.GetFiles(_folder, "*.json"));
    }

    [Fact]
    public async Task File_PollingSeesAWaitingFileManyTimes_AndDeliversItOnce()
    {
        var endpoint = CreateFileEndPoint(TimeSpan.FromMilliseconds(20));
        var calls = 0;
        await endpoint.PostDeferredAsync("{}"u8.ToArray(), DateTimeOffset.UtcNow.AddMilliseconds(500), Ct);

        await endpoint.StartListeningAsync((_, _) =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(ProcessResult.Completed);
        }, Ct);
        await DeferredContract.WaitUntilAsync(() => Volatile.Read(ref calls) > 0, Timeout);
        await Task.Delay(200, Ct);
        await endpoint.StopListeningAsync();

        Assert.Equal(1, Volatile.Read(ref calls));
    }

    [Fact]
    public async Task File_SurvivesARestart_WaitingFileIsDeliveredByTheNextListener()
    {
        var first = CreateFileEndPoint();
        using (var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct))
        {
            await first.PostDeferredAsync("{}"u8.ToArray(), DateTimeOffset.UtcNow.AddSeconds(1), Ct);
            await first.StartListeningAsync((_, _) => throw new InvalidOperationException("Delivered early"), stop.Token);
            await Task.Delay(100, Ct);
            await stop.CancelAsync();
            await first.StopListeningAsync();
        }

        var second = CreateFileEndPoint();
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await second.StartListeningAsync((_, _) =>
        {
            delivered.TrySetResult();
            return Task.FromResult(ProcessResult.Completed);
        }, Ct);

        await delivered.Task.WaitAsync(Timeout, Ct);
        await second.StopListeningAsync();
    }

    [Fact]
    public async Task File_NameThatOnlyLooksDeferred_IsDeliveredAtOnce()
    {
        Directory.CreateDirectory(_folder);
        await File.WriteAllTextAsync(Path.Combine(_folder, "deferred-report_final.json"), "{}", Ct);

        var items = new List<string>();
        await foreach (var item in CreateFileEndPoint().ReadAsync(Ct)) items.Add(item);

        Assert.Single(items);
    }

    // --- SQLite ---

    private string SqliteConnectionString => $"Data Source={Path.Combine(_folder, "queue.db")};Pooling=False";

    private SqliteEndPoint CreateSqliteEndPoint(int maxRetryCount = 5, int batchSize = 10)
    {
        Directory.CreateDirectory(_folder);
        return new SqliteEndPoint("Sql", new SqliteSettings
        {
            ConnectionString = SqliteConnectionString,
            PollInterval = TimeSpan.FromMilliseconds(20),
            MaxRetryCount = maxRetryCount,
            BatchSize = batchSize
        }, NullLogger<SqliteEndPoint>.Instance);
    }

    [Fact]
    public async Task Sqlite_DeferredMessageWaits_OtherMessagesDoNot()
    {
        await using var endpoint = CreateSqliteEndPoint();
        await DeferredContract.DeferredMessageWaits_OtherMessagesDoNot(endpoint);
    }

    [Fact]
    public async Task Sqlite_RetryCountStartsAtTheFirstDelivery()
    {
        await using var endpoint = CreateSqliteEndPoint(maxRetryCount: 2);
        var calls = 0;
        await endpoint.PostDeferredAsync("{}"u8.ToArray(), DateTimeOffset.UtcNow.AddMilliseconds(200), Ct);

        await endpoint.StartListeningAsync((_, _) =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(ProcessResult.Retry(description: "again"));
        }, Ct);

        await DeferredContract.WaitUntilAsync(() => Volatile.Read(ref calls) >= 2, Timeout);
        await Task.Delay(200, Ct);
        await endpoint.StopListeningAsync();
        Assert.Equal(2, Volatile.Read(ref calls));
    }

    [Fact]
    public async Task Sqlite_MoreDueThanABatch_EachIsDeliveredOnce()
    {
        await using var endpoint = CreateSqliteEndPoint(batchSize: 10);
        var received = new ConcurrentBag<string>();
        for (var i = 0; i < 35; i++)
            await endpoint.PostDeferredAsync(Encoding.UTF8.GetBytes(i.ToString()), DateTimeOffset.UtcNow.AddMinutes(-1), Ct);

        await endpoint.StartListeningAsync((raw, _) =>
        {
            received.Add(Encoding.UTF8.GetString(raw));
            return Task.FromResult(ProcessResult.Completed);
        }, Ct);

        await DeferredContract.WaitUntilAsync(() => received.Count >= 35, Timeout);
        await endpoint.StopListeningAsync();
        Assert.Equal(35, received.Distinct().Count());
        Assert.Equal(35, received.Count);
    }

    [Fact]
    public async Task Sqlite_LargeDeferredBacklog_StaysOutOfTheQueueTable_AndIsNotClaimed()
    {
        await using var endpoint = CreateSqliteEndPoint();
        for (var i = 0; i < 200; i++)
            await endpoint.PostDeferredAsync("{}"u8.ToArray(), DateTimeOffset.UtcNow.AddHours(1), Ct);
        await endpoint.PostAsync("""{"n":"now"}"""u8.ToArray(), Ct);

        var items = new List<string>();
        await foreach (var item in endpoint.ReadAsync(Ct)) items.Add(item);

        Assert.Contains("now", Assert.Single(items));
        Assert.Equal(1L, await ScalarAsync("SELECT COUNT(*) FROM NymBrokerMessages"));
        Assert.Equal(200L, await ScalarAsync("SELECT COUNT(*) FROM NymBrokerMessages_deferred"));
    }

    [Fact]
    public async Task Sqlite_MovingDueMessages_UsesTheDueIndex()
    {
        await using var conn = await SqliteTestDb.OpenAsync("Data Source=:memory:");
        await SqliteTestDb.ExecuteAsync(conn, SqliteQueueSql.CreateTable("Q"));

        foreach (var sql in new[] { SqliteQueueSql.CopyDue("Q"), SqliteQueueSql.DeleteDue("Q") })
        {
            await using var command = conn.CreateCommand();
            command.CommandText = "EXPLAIN QUERY PLAN " + sql;
            command.Parameters.AddWithValue("$now", 0);
            command.Parameters.AddWithValue("$batchSize", 10);
            var plan = new List<string>();
            await using var reader = await command.ExecuteReaderAsync(Ct);
            while (await reader.ReadAsync(Ct))
                plan.Add(reader.GetString(3));

            Assert.Contains(plan, line => line.Contains("IX_Q_deferred_Due"));
        }
    }

    [Fact]
    public async Task Sqlite_ExistingTableWithoutTheDeferredTable_GetsIt()
    {
        Directory.CreateDirectory(_folder);
        await using (var conn = new SqliteConnection(SqliteConnectionString))
        {
            await conn.OpenAsync(Ct);
            var create = SqliteQueueSql.CreateTable("NymBrokerMessages");
            await SqliteTestDb.ExecuteAsync(conn, create);
            await SqliteTestDb.ExecuteAsync(conn, "DROP TABLE NymBrokerMessages_deferred");
        }

        await using var endpoint = CreateSqliteEndPoint();
        await endpoint.PostDeferredAsync("{}"u8.ToArray(), DateTimeOffset.UtcNow.AddHours(1), Ct);

        Assert.Equal(1L, await ScalarAsync("SELECT COUNT(*) FROM NymBrokerMessages_deferred"));
    }

    [Fact]
    public async Task Sqlite_MissingDeferredTable_OrdinaryMessagesStillWork_AndTheErrorIsLogged()
    {
        Directory.CreateDirectory(_folder);
        await using (var conn = new SqliteConnection(SqliteConnectionString))
        {
            await conn.OpenAsync(Ct);
            await SqliteTestDb.ExecuteAsync(conn, SqliteQueueSql.CreateTable("Hand"));
            await SqliteTestDb.ExecuteAsync(conn, "DROP TABLE Hand_deferred");
        }
        var logger = new CapturingLogger<SqliteEndPoint>();
        await using var endpoint = new SqliteEndPoint("Sql", new SqliteSettings
        {
            ConnectionString = SqliteConnectionString,
            TableName = "Hand",
            AutoCreateTable = false,
            PollInterval = TimeSpan.FromMilliseconds(20)
        }, logger);
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await endpoint.PostAsync("{}"u8.ToArray(), Ct);
        await endpoint.StartListeningAsync((_, _) =>
        {
            delivered.TrySetResult();
            return Task.FromResult(ProcessResult.Completed);
        }, Ct);

        await delivered.Task.WaitAsync(Timeout, Ct);
        await endpoint.StopListeningAsync();
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error && e.Message.Contains("deferred"));
        await Assert.ThrowsAsync<SqliteException>(() => endpoint.PostDeferredAsync("{}"u8.ToArray(), DateTimeOffset.UtcNow.AddHours(1), Ct));
    }

    private async Task<long> ScalarAsync(string sql)
    {
        await using var conn = new SqliteConnection(SqliteConnectionString);
        await conn.OpenAsync(Ct);
        await using var command = conn.CreateCommand();
        command.CommandText = sql;
        return (long)(await command.ExecuteScalarAsync(Ct))!;
    }
}
