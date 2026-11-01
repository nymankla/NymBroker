using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Core.Factory.Configuration;
using NymBroker.Endpoint.MySql;
using NymBroker.Endpoint.Postgres;
using NymBroker.Endpoint.Sqlite;
using NymBroker.Endpoint.SqlServer;

namespace NymBroker.Tests;

/// <summary>What every SQL endpoint must do with old rows. Each endpoint's tests supply how to set a row up and read the table.</summary>
internal static class RetentionContract
{
    public static readonly TimeSpan CompletedRetention = TimeSpan.FromHours(1);
    public static readonly TimeSpan FailedRetention = TimeSpan.FromHours(3);
    public static readonly TimeSpan CleanupInterval = TimeSpan.FromMilliseconds(100);
    public const int CleanupBatchSize = 2;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Puts a row in a status, finished (and created) this many minutes ago. An InProgress row holds a lease an hour ahead.</summary>
    public delegate Task SetRow(long queueId, QueueMessageStatus status, int ageMinutes);

    /// <param name="endpoint">An empty table, with the retention settings above.</param>
    /// <param name="logger">The endpoint's logger.</param>
    /// <param name="setRow">Sets a row up directly in the table.</param>
    /// <param name="remainingIds">The queue ids left in the table, ascending, comma separated.</param>
    /// <param name="deferredCount">The number of rows in the deferred table.</param>
    public static async Task DeletesOnlyOldFinishedRows<TEndPoint>(
        TEndPoint endpoint, CapturingLogger<TEndPoint> logger, SetRow setRow, Func<Task<string>> remainingIds, Func<Task<long>> deferredCount)
        where TEndPoint : IDeferredEndPoint, IEndPointEventDriven
    {
        for (var i = 0; i < 8; i++)
            await endpoint.PostAsync("{}"u8.ToArray(), Ct);
        await endpoint.PostDeferredAsync("{}"u8.ToArray(), DateTimeOffset.UtcNow.AddDays(1), Ct);

        await setRow(1, QueueMessageStatus.Completed, 120);    // deleted: more than a batch of old Completed rows
        await setRow(2, QueueMessageStatus.Completed, 120);    // deleted
        await setRow(3, QueueMessageStatus.Completed, 120);    // deleted
        await setRow(4, QueueMessageStatus.Completed, 30);     // kept: within CompletedRetention
        await setRow(5, QueueMessageStatus.Failed, 120);       // kept: older than CompletedRetention, within FailedRetention
        await setRow(6, QueueMessageStatus.Failed, 240);       // deleted
        await setRow(7, QueueMessageStatus.InProgress, 10_000); // kept: being handled, however old
        await setRow(8, QueueMessageStatus.Pending, 10_000);   // kept: delivered, however old

        var delivered = 0;
        await endpoint.StartListeningAsync((_, _) =>
        {
            Interlocked.Increment(ref delivered);
            return Task.FromResult(ProcessResult.Completed);
        }, Ct);

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        while (await remainingIds() != "4,5,7,8" || Volatile.Read(ref delivered) == 0)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"Rows left: {await remainingIds()}; delivered: {Volatile.Read(ref delivered)}.");
            await Task.Delay(25, Ct);
        }
        await endpoint.StopListeningAsync();

        Assert.Equal(1, Volatile.Read(ref delivered));
        Assert.Equal(1, await deferredCount());
        Assert.DoesNotContain(logger.Entries, e => e.Level >= LogLevel.Error);
        // All three in one pass, so a full batch made it go on at once: the row count the statement returned is right.
        Assert.Contains(logger.Entries, e => e.Message.StartsWith("Deleted 3 old Completed row(s)", StringComparison.Ordinal));
        Assert.Contains(logger.Entries, e => e.Message.StartsWith("Deleted 1 old Failed row(s)", StringComparison.Ordinal));
    }
}

public sealed class SqliteRetentionTests : IDisposable
{
    private readonly SqliteTestDb.TempFile _db = new();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public void Dispose() => _db.Dispose();

    private SqliteEndPoint CreateEndPoint(ILogger<SqliteEndPoint>? logger = null, TimeSpan? completedRetention = null)
        => new("Sql", new SqliteSettings
        {
            ConnectionString = _db.ConnectionString,
            PollInterval = TimeSpan.FromMilliseconds(10),
            CompletedRetention = completedRetention ?? RetentionContract.CompletedRetention,
            FailedRetention = RetentionContract.FailedRetention,
            CleanupInterval = RetentionContract.CleanupInterval,
            CleanupBatchSize = RetentionContract.CleanupBatchSize
        }, logger ?? new CapturingLogger<SqliteEndPoint>());

    private Task SetRowAsync(long queueId, QueueMessageStatus status, int ageMinutes)
    {
        var finishedAt = $"unixepoch() - {ageMinutes * 60}";
        return SqliteTestDb.ExecuteAsync(_db.ConnectionString, $"""
            UPDATE NymBrokerMessages
               SET Status = {(int)status},
                   CreatedAtUtc = {finishedAt},
                   CompletedAtUtc = {(status == QueueMessageStatus.Completed ? finishedAt : "NULL")},
                   FailedAtUtc = {(status == QueueMessageStatus.Failed ? finishedAt : "NULL")},
                   LockedUntilUtc = {(status == QueueMessageStatus.InProgress ? "unixepoch() + 3600" : "NULL")}
             WHERE QueueId = {queueId}
            """);
    }

    private Task<string> RemainingIdsAsync() => SqliteTestDb.ScalarAsync<string>(_db.ConnectionString,
        "SELECT COALESCE(group_concat(QueueId), '') FROM (SELECT QueueId FROM NymBrokerMessages ORDER BY QueueId)");

    [Fact]
    public async Task DeletesOnlyOldFinishedRows()
    {
        var logger = new CapturingLogger<SqliteEndPoint>();
        await using var endpoint = CreateEndPoint(logger);
        await RetentionContract.DeletesOnlyOldFinishedRows(endpoint, logger, SetRowAsync, RemainingIdsAsync,
            () => SqliteTestDb.ScalarAsync<long>(_db.ConnectionString, "SELECT COUNT(*) FROM NymBrokerMessages_deferred"));
    }

    [Fact]
    public async Task ZeroRetention_KeepsThatStatus_AndTheOtherIsStillDeleted()
    {
        var logger = new CapturingLogger<SqliteEndPoint>();
        await using var endpoint = CreateEndPoint(logger, completedRetention: TimeSpan.Zero);
        await endpoint.PostAsync("{}"u8.ToArray(), Ct);
        await endpoint.PostAsync("{}"u8.ToArray(), Ct);
        await SetRowAsync(1, QueueMessageStatus.Completed, 1_000_000);
        await SetRowAsync(2, QueueMessageStatus.Failed, 1_000_000);

        await endpoint.StartListeningAsync((_, _) => Task.FromResult(ProcessResult.Completed), Ct);
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (await RemainingIdsAsync() != "1")
        {
            Assert.True(DateTime.UtcNow < deadline, $"Rows left: {await RemainingIdsAsync()}");
            await Task.Delay(25, Ct);
        }
        await endpoint.StopListeningAsync();

        Assert.Equal("1", await RemainingIdsAsync());
    }

    [Fact]
    public async Task WithoutAListener_NothingIsDeleted()
    {
        await using var endpoint = CreateEndPoint();
        await endpoint.PostAsync("{}"u8.ToArray(), Ct);
        await SetRowAsync(1, QueueMessageStatus.Completed, 1_000_000);

        await Task.Delay(RetentionContract.CleanupInterval * 3, Ct);

        Assert.Equal("1", await RemainingIdsAsync());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Delete_UsesItsPartialIndex(bool completed)
    {
        await using var conn = await SqliteTestDb.OpenAsync("Data Source=:memory:");
        await SqliteTestDb.ExecuteAsync(conn, SqliteQueueSql.CreateTable("Q"));
        await using var command = conn.CreateCommand();
        command.CommandText = "EXPLAIN QUERY PLAN " + (completed ? SqliteQueueSql.DeleteCompleted("Q") : SqliteQueueSql.DeleteFailed("Q"));
        command.Parameters.AddWithValue("$olderThanSeconds", 60);
        command.Parameters.AddWithValue("$batchSize", 10);
        var plan = new List<string>();
        await using (var reader = await command.ExecuteReaderAsync(Ct))
            while (await reader.ReadAsync(Ct))
                plan.Add(reader.GetString(3));

        Assert.Contains(plan, line => line.Contains(completed ? SqliteQueueSql.CompletedIndexName("Q") : SqliteQueueSql.FailedIndexName("Q")));
    }

    [Fact]
    public async Task TableFromAnOlderVersion_GetsTheIndexes()
    {
        await SqliteTestDb.ExecuteAsync(_db.ConnectionString, SqliteQueueSql.CreateTable("NymBrokerMessages"));
        await SqliteTestDb.ExecuteAsync(_db.ConnectionString, """
            DROP INDEX IX_NymBrokerMessages_Completed;
            DROP INDEX IX_NymBrokerMessages_Failed;
            """);

        await using var endpoint = CreateEndPoint();
        await endpoint.PostAsync("{}"u8.ToArray(), Ct);

        Assert.Equal(2, await SqliteTestDb.ScalarAsync<int>(_db.ConnectionString,
            "SELECT COUNT(*) FROM sqlite_master WHERE name IN ('IX_NymBrokerMessages_Completed', 'IX_NymBrokerMessages_Failed')"));
    }
}

public sealed class LeasedQueueCleanupTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private sealed class Settings : ILeasedQueueSettings
    {
        public int BatchSize => 10;
        public TimeSpan PollInterval => TimeSpan.FromMilliseconds(5);
        public TimeSpan LeaseTimeout => TimeSpan.FromMinutes(1);
        public int MaxRetryCount => 3;
        public bool UseNativeDeadLetter => true;
        public TimeSpan CompletedRetention { get; init; } = TimeSpan.FromHours(1);
        public TimeSpan FailedRetention { get; init; } = TimeSpan.FromHours(2);
        public TimeSpan CleanupInterval { get; init; } = TimeSpan.FromMilliseconds(20);
        public int CleanupBatchSize { get; init; } = 10;
    }

    /// <summary>A transport that was written before there was a cleanup: only the required members.</summary>
    private sealed class MinimalSettings : ILeasedQueueSettings
    {
        public int BatchSize => 10;
        public TimeSpan PollInterval => TimeSpan.FromMilliseconds(5);
        public TimeSpan LeaseTimeout => TimeSpan.FromMinutes(1);
        public int MaxRetryCount => 3;
        public bool UseNativeDeadLetter => true;
    }

    private sealed class FakeListener(ILeasedQueueSettings settings, ILogger logger, Func<QueueMessageStatus, int, CancellationToken, Task<int>>? delete = null)
        : LeasedQueueListener("Fake", settings, logger)
    {
        private int _claims;

        public ConcurrentQueue<(QueueMessageStatus Status, int OlderThanSeconds)> Deletes { get; } = new();
        public int Claims => Volatile.Read(ref _claims);

        protected override Task<IReadOnlyList<QueueMessage>> FinalizeAndClaimAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
        {
            Interlocked.Increment(ref _claims);
            return Task.FromResult<IReadOnlyList<QueueMessage>>([]);
        }

        protected override Task FinalizeAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct) => Task.CompletedTask;

        protected override Task<int> DeleteFinishedAsync(QueueMessageStatus status, int olderThanSeconds, CancellationToken ct)
        {
            Deletes.Enqueue((status, olderThanSeconds));
            return delete?.Invoke(status, olderThanSeconds, ct) ?? Task.FromResult(0);
        }

        public void Start() => Start((_, _) => Task.FromResult(ProcessResult.Completed), Ct);
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "Condition was not met in time.");
            await Task.Delay(5, Ct);
        }
    }

    [Fact]
    public async Task GoesOnWhileABatchIsFull_ThenDeletesTheOtherStatus()
    {
        var results = new Queue<int>([10, 10, 3]);
        var listener = new FakeListener(new Settings { CleanupBatchSize = 10 }, new CapturingLogger<FakeListener>(),
            (status, _, _) => Task.FromResult(status == QueueMessageStatus.Completed && results.Count > 0 ? results.Dequeue() : 0));

        listener.Start();
        await WaitUntilAsync(() => listener.Deletes.Count >= 4);
        await listener.StopAsync();

        Assert.Equal(
            [(QueueMessageStatus.Completed, 3600), (QueueMessageStatus.Completed, 3600), (QueueMessageStatus.Completed, 3600), (QueueMessageStatus.Failed, 7200)],
            listener.Deletes.Take(4));
    }

    [Fact]
    public async Task Failure_IsLogged_DoesNotStopTheOtherStatusOrTheLoop_AndIsRetried()
    {
        var logger = new CapturingLogger<FakeListener>();
        var failures = 1;
        var listener = new FakeListener(new Settings(), logger, (status, _, _) =>
            status == QueueMessageStatus.Completed && Interlocked.Exchange(ref failures, 0) == 1
                ? throw new InvalidOperationException("permission denied")
                : Task.FromResult(0));

        listener.Start();
        await WaitUntilAsync(() => listener.Deletes.Count(d => d.Status == QueueMessageStatus.Completed) >= 2 && listener.Claims > 0);
        var claims = listener.Claims;
        await WaitUntilAsync(() => listener.Claims > claims);
        await listener.StopAsync();

        var error = Assert.Single(logger.Entries, e => e.Level == LogLevel.Error);
        Assert.IsType<InvalidOperationException>(error.Exception);
        Assert.Equal(QueueMessageStatus.Failed, listener.Deletes.Skip(1).First().Status);
    }

    [Fact]
    public async Task Stop_CancelsADeleteInProgress_WithoutAnError()
    {
        var logger = new CapturingLogger<FakeListener>();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var listener = new FakeListener(new Settings(), logger, async (_, _, ct) =>
        {
            started.TrySetResult();
            await Task.Delay(System.Threading.Timeout.Infinite, ct);
            return 0;
        });

        listener.Start();
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        await listener.StopAsync();
        var deletes = listener.Deletes.Count;
        await Task.Delay(100, Ct);

        Assert.Equal(deletes, listener.Deletes.Count);
        Assert.DoesNotContain(logger.Entries, e => e.Level >= LogLevel.Warning);
    }

    [Theory]
    [InlineData(0, 3600, 3600)]     // no interval
    [InlineData(20, 0, 0)]          // nothing to delete
    public async Task NoCleanup_WhenDisabled(int intervalMs, int completedSeconds, int failedSeconds)
    {
        var listener = new FakeListener(new Settings
        {
            CleanupInterval = TimeSpan.FromMilliseconds(intervalMs),
            CompletedRetention = TimeSpan.FromSeconds(completedSeconds),
            FailedRetention = TimeSpan.FromSeconds(failedSeconds)
        }, new CapturingLogger<FakeListener>());

        listener.Start();
        await WaitUntilAsync(() => listener.Claims > 3);
        await Task.Delay(100, Ct);
        await listener.StopAsync();

        Assert.Empty(listener.Deletes);
    }

    [Fact]
    public async Task ATransportWithoutTheSettings_HasNoCleanup()
    {
        var listener = new FakeListener(new MinimalSettings(), new CapturingLogger<FakeListener>());

        listener.Start();
        await WaitUntilAsync(() => listener.Claims > 3);
        await listener.StopAsync();

        Assert.Empty(listener.Deletes);
    }

    [Fact]
    public async Task RetentionIsRoundedUp_ToWholeSeconds()
    {
        var listener = new FakeListener(new Settings { CompletedRetention = TimeSpan.FromMilliseconds(1500), FailedRetention = TimeSpan.Zero },
            new CapturingLogger<FakeListener>());

        listener.Start();
        await WaitUntilAsync(() => !listener.Deletes.IsEmpty);
        await listener.StopAsync();

        Assert.All(listener.Deletes, d => Assert.Equal((QueueMessageStatus.Completed, 2), d));
    }

    public static TheoryData<TimeSpan, TimeSpan, TimeSpan, int> InvalidSettings => new()
    {
        { TimeSpan.FromSeconds(-1), TimeSpan.Zero, TimeSpan.FromMinutes(1), 1 },
        { TimeSpan.Zero, TimeSpan.FromSeconds(-1), TimeSpan.FromMinutes(1), 1 },
        { TimeSpan.FromSeconds(int.MaxValue + 1L), TimeSpan.Zero, TimeSpan.FromMinutes(1), 1 },
        { TimeSpan.Zero, TimeSpan.MaxValue, TimeSpan.FromMinutes(1), 1 },
        { TimeSpan.FromDays(1), TimeSpan.FromDays(1), TimeSpan.FromSeconds(-1), 1 },
        { TimeSpan.FromDays(1), TimeSpan.FromDays(1), TimeSpan.FromTicks(1), 1 },
        { TimeSpan.FromDays(1), TimeSpan.FromDays(1), TimeSpan.FromDays(50), 1 },
        { TimeSpan.FromDays(1), TimeSpan.FromDays(1), TimeSpan.FromMinutes(1), 0 }
    };

    [Theory]
    [MemberData(nameof(InvalidSettings))]
    public void InvalidSettings_FailWhenTheEndpointIsCreated(TimeSpan completed, TimeSpan failed, TimeSpan interval, int batchSize)
    {
        Assert.Throws<ArgumentException>(() => new SqliteEndPoint("Sql", new SqliteSettings
        {
            ConnectionString = "Data Source=:memory:",
            CompletedRetention = completed,
            FailedRetention = failed,
            CleanupInterval = interval,
            CleanupBatchSize = batchSize
        }, new CapturingLogger<SqliteEndPoint>()));
    }

    public static TheoryData<ILeasedQueueSettings> EverySqlEndpoint =>
        [new SqliteSettings(), new PostgresSettings(), new SqlServerSettings(), new MySqlSettings()];

    [Theory]
    [MemberData(nameof(EverySqlEndpoint))]
    public void RetentionIsOnByDefault(ILeasedQueueSettings settings)
    {
        Assert.Equal(TimeSpan.FromDays(30), settings.CompletedRetention);
        Assert.Equal(TimeSpan.FromDays(90), settings.FailedRetention);
        Assert.Equal(TimeSpan.FromMinutes(10), settings.CleanupInterval);
        Assert.Equal(1000, settings.CleanupBatchSize);
    }

    [Fact]
    public void SettingsCanComeFromTheConfigurationFile()
    {
        var configuration = new EndPointConfiguration
        {
            Config = JsonDocument.Parse("""{ "completedRetention": "7.00:00:00", "failedRetention": "00:00:00", "cleanupInterval": "00:01:00", "cleanupBatchSize": 50 }""").RootElement
        };

        var settings = configuration.GetSettings<SqliteSettings>();

        Assert.Equal(TimeSpan.FromDays(7), settings.CompletedRetention);
        Assert.Equal(TimeSpan.Zero, settings.FailedRetention);
        Assert.Equal(TimeSpan.FromMinutes(1), settings.CleanupInterval);
        Assert.Equal(50, settings.CleanupBatchSize);
    }
}
