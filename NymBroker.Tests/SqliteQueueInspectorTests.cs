using NymBroker.Core;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Endpoint.Queue;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core.DI;
using NymBroker.Endpoint.Sqlite;

namespace NymBroker.Tests;

/// <summary>The read-only <see cref="IQueueInspector"/> of <see cref="SqliteEndPoint"/>.</summary>
public sealed class SqliteQueueInspectorTests : IDisposable
{
    private readonly SqliteTestDb.TempFile _db = new();
    private const string Table = "Inspected";

    private SqliteEndPoint CreateEndPoint(int maxRetryCount = 5) => new("inspect",
        new SqliteSettings { ConnectionString = _db.ConnectionString, TableName = Table, MaxRetryCount = maxRetryCount, PollInterval = TimeSpan.FromMilliseconds(10) },
        NullLogger<SqliteEndPoint>.Instance);

    public void Dispose() => _db.Dispose();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task EmptyQueue_HasZeroCounts_AndEmptyPages()
    {
        await using var ep = CreateEndPoint();

        Assert.Equal(new QueueCounts(0, 0, 0, 0, 0), await ep.GetCountsAsync(Ct));
        foreach (var state in new[] { QueueInspectionState.Pending, QueueInspectionState.InProgress, QueueInspectionState.Deferred, QueueInspectionState.Failed })
        {
            var page = await ep.GetMessagesAsync(state, ct: Ct);
            Assert.Empty(page.Messages);
            Assert.Null(page.NextCursor);
        }
    }

    [Fact]
    public async Task Counts_SplitByState()
    {
        await using var ep = CreateEndPoint();
        await ep.PostAsync("a"u8.ToArray(), Ct);
        await ep.PostAsync("b"u8.ToArray(), Ct);
        await ep.PostDeferredAsync("c"u8.ToArray(), DateTimeOffset.UtcNow.AddHours(1), Ct);
        await SqliteTestDb.ExecuteAsync(_db.ConnectionString,
            $"INSERT INTO {Table} (MessageId, Status, AttemptCount, Payload, FailedAtUtc, LastError) VALUES (randomblob(16), 3, 5, 'f', unixepoch(), 'boom');" +
            $"INSERT INTO {Table} (MessageId, Status, AttemptCount, Payload, CompletedAtUtc) VALUES (randomblob(16), 2, 1, 'd', unixepoch());" +
            $"INSERT INTO {Table} (MessageId, Status, AttemptCount, Payload, LockedUntilUtc) VALUES (randomblob(16), 1, 1, 'i', unixepoch() + 600);");

        var counts = await ep.GetCountsAsync(Ct);

        Assert.Equal(new QueueCounts(Pending: 2, InProgress: 1, Deferred: 1, Failed: 1, Completed: 1), counts);
        Assert.Equal(4, counts.Active);
    }

    [Fact]
    public async Task Pending_PagesInPostOrder_WithCursor()
    {
        await using var ep = CreateEndPoint();
        for (var i = 0; i < 5; i++)
            await ep.PostAsync(System.Text.Encoding.UTF8.GetBytes($"m{i}"), Ct);

        var first = await ep.GetMessagesAsync(QueueInspectionState.Pending, 2, ct: Ct);
        var second = await ep.GetMessagesAsync(QueueInspectionState.Pending, 2, first.NextCursor, ct: Ct);
        var third = await ep.GetMessagesAsync(QueueInspectionState.Pending, 2, second.NextCursor, ct: Ct);

        Assert.Equal(["m0", "m1"], Texts(first));
        Assert.Equal(["m2", "m3"], Texts(second));
        Assert.Equal(["m4"], Texts(third));
        Assert.NotNull(first.NextCursor);
        Assert.Null(third.NextCursor);
        Assert.All(first.Messages.Concat(second.Messages).Concat(third.Messages), m =>
        {
            Assert.Equal(QueueInspectionState.Pending, m.State);
            Assert.Equal(0, m.Attempts);
            Assert.Null(m.DeliverAt);
            Assert.NotEqual(Guid.Empty, Guid.Parse(m.Id));
        });
    }

    [Fact]
    public async Task ExactPageSize_HasNoNextCursor()
    {
        await using var ep = CreateEndPoint();
        await ep.PostAsync("a"u8.ToArray(), Ct);
        await ep.PostAsync("b"u8.ToArray(), Ct);

        var page = await ep.GetMessagesAsync(QueueInspectionState.Pending, 2, ct: Ct);

        Assert.Equal(2, page.Messages.Count);
        Assert.Null(page.NextCursor);
    }

    [Fact]
    public async Task Deferred_ListsDeliveryTime_OrderedByDueTime_AndKeepsIdWhenPromoted()
    {
        await using var ep = CreateEndPoint();
        var late = DateTimeOffset.UtcNow.AddHours(2);
        var soon = DateTimeOffset.UtcNow.AddHours(1);
        await ep.PostDeferredAsync("late"u8.ToArray(), late, Ct);
        await ep.PostDeferredAsync("soon"u8.ToArray(), soon, Ct);

        var page = await ep.GetMessagesAsync(QueueInspectionState.Deferred, ct: Ct);

        Assert.Equal(["soon", "late"], Texts(page));
        Assert.All(page.Messages, m => Assert.Equal(QueueInspectionState.Deferred, m.State));
        Assert.InRange(page.Messages[0].DeliverAt!.Value - soon, TimeSpan.FromMilliseconds(-2), TimeSpan.FromMilliseconds(2));

        var paged = await ep.GetMessagesAsync(QueueInspectionState.Deferred, 1, ct: Ct);
        var rest = await ep.GetMessagesAsync(QueueInspectionState.Deferred, 1, paged.NextCursor, ct: Ct);
        Assert.Equal(["soon"], Texts(paged));
        Assert.Equal(["late"], Texts(rest));
        Assert.Null(rest.NextCursor);

        var id = page.Messages[0].Id;
        await SqliteTestDb.ExecuteAsync(_db.ConnectionString, $"UPDATE {Table}_deferred SET DeliverAtUtc = 0");
        await ep.StartListeningAsync(async (_, ct) => { await Task.Delay(Timeout.Infinite, ct); return ProcessResult.Completed; }, Ct);   // promotes, then holds the claim
        await WaitUntilAsync(async () => (await ep.GetCountsAsync(Ct)).Deferred == 0);
        await ep.StopListeningAsync();
        var promoted = (await ep.GetMessagesAsync(QueueInspectionState.InProgress, ct: Ct)).Messages.Concat(
            (await ep.GetMessagesAsync(QueueInspectionState.Pending, ct: Ct)).Messages);
        Assert.Contains(promoted, m => m.Id == id);
    }

    [Fact]
    public async Task Failed_HasErrorAttemptsAndTime_AndFiltersBySince_AndPages()
    {
        await using var ep = CreateEndPoint();
        await ep.GetCountsAsync(Ct);   // creates the table
        await SqliteTestDb.ExecuteAsync(_db.ConnectionString,
            $"INSERT INTO {Table} (MessageId, Status, AttemptCount, Payload, FailedAtUtc, LastError) VALUES (randomblob(16), 3, 3, 'old', 1000, 'e1');" +
            $"INSERT INTO {Table} (MessageId, Status, AttemptCount, Payload, FailedAtUtc, LastError) VALUES (randomblob(16), 3, 2, 'mid', 2000, 'e2');" +
            $"INSERT INTO {Table} (MessageId, Status, AttemptCount, Payload, FailedAtUtc, LastError) VALUES (randomblob(16), 3, 1, 'mid2', 2000, 'e3');" +
            $"INSERT INTO {Table} (MessageId, Status, AttemptCount, Payload, FailedAtUtc, LastError) VALUES (randomblob(16), 3, 1, 'new', 3000, 'e4');");

        var all = await ep.GetMessagesAsync(QueueInspectionState.Failed, ct: Ct);
        Assert.Equal(["old", "mid", "mid2", "new"], Texts(all));
        Assert.Equal("e1", all.Messages[0].LastError);
        Assert.Equal(3, all.Messages[0].Attempts);
        Assert.Equal(DateTimeOffset.FromUnixTimeSeconds(1000), all.Messages[0].FailedAt);

        var since = await ep.GetMessagesAsync(QueueInspectionState.Failed, failedSince: DateTimeOffset.FromUnixTimeSeconds(2000), ct: Ct);
        Assert.Equal(["mid", "mid2", "new"], Texts(since));

        // Two rows share a failure second: the cursor must not skip or repeat either.
        var p1 = await ep.GetMessagesAsync(QueueInspectionState.Failed, 2, ct: Ct);
        var p2 = await ep.GetMessagesAsync(QueueInspectionState.Failed, 2, p1.NextCursor, ct: Ct);
        Assert.Equal(["old", "mid"], Texts(p1));
        Assert.Equal(["mid2", "new"], Texts(p2));
    }

    [Fact]
    public async Task NativeDeadLetter_AppearsAsFailed_WithItsReason()
    {
        await using var ep = CreateEndPoint(maxRetryCount: 1);
        await ep.PostAsync("{}"u8.ToArray(), Ct);
        await ep.StartListeningAsync((_, _) => Task.FromResult(ProcessResult.DeadLetter(DeadLetterReasons.Expired, "too old")), Ct);
        await WaitUntilAsync(async () => (await ep.GetCountsAsync(Ct)).Failed == 1);
        await ep.StopListeningAsync();

        var failed = (await ep.GetMessagesAsync(QueueInspectionState.Failed, ct: Ct)).Messages.Single();

        Assert.Equal("Expired: too old", failed.LastError);
        Assert.Equal(1, failed.Attempts);
        Assert.Equal(0, (await ep.GetCountsAsync(Ct)).Active);
    }

    [Fact]
    public async Task InvalidArguments_Throw()
    {
        await using var ep = CreateEndPoint();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => ep.GetMessagesAsync(QueueInspectionState.Completed, ct: Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => ep.GetMessagesAsync(QueueInspectionState.Pending, 0, ct: Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => ep.GetMessagesAsync(QueueInspectionState.Pending, IQueueInspector.MaxPageSize + 1, ct: Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => ep.GetMessagesAsync(QueueInspectionState.Pending, cursor: "abc", ct: Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => ep.GetMessagesAsync(QueueInspectionState.Failed, cursor: "5", ct: Ct));
    }

    [Fact]
    public async Task Broker_ReturnsInspector_ForSqlite_AndNullForOtherTransports()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .AddSqliteEndPoint("sql", new SqliteSettings { ConnectionString = _db.ConnectionString, TableName = Table })
            .AddMemoryEndPoint("mem", mode: EndpointMode.WriteOnly)
            .Build();
        await using var provider = services.BuildServiceProvider();
        var broker = provider.GetRequiredService<INymBroker>();
        Assert.NotNull(broker.GetQueueInspector("sql"));
        Assert.Null(broker.GetQueueInspector("mem"));
        Assert.Throws<InvalidOperationException>(() => broker.GetQueueInspector("nope"));
    }

    private static string[] Texts(QueuePage page) =>
        page.Messages.Select(m => System.Text.Encoding.UTF8.GetString(m.Payload.Span)).ToArray();

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!await condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "Condition not met in time.");
            await Task.Delay(20, TestContext.Current.CancellationToken);
        }
    }
}
