using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Endpoint.SqlServer;

namespace NymBroker.Tests;

/// <summary>The read-only <see cref="IQueueInspector"/> of <see cref="SqlServerEndPoint"/>.</summary>
[Collection("SQL Server integration")]
public sealed class SqlServerQueueInspectorTests : IAsyncLifetime
{
    private const string ConnectionStringVariable = "NYMBROKER_SQLSERVER_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);
    private readonly string _tableName = $"dbo.nb_inspect_{Guid.NewGuid():N}";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private SqlServerEndPoint CreateEndPoint() => new("inspect",
        new SqlServerSettings
        {
            ConnectionString = ConnectionString!,
            TableName = _tableName,
            PollInterval = TimeSpan.FromMilliseconds(20)
        }, NullLogger<SqlServerEndPoint>.Instance);

    [Fact]
    public async Task EmptyQueue_HasZeroCounts_AndEmptyPages()
    {
        RequireSqlServer();
        await using var ep = CreateEndPoint();

        Assert.Equal(new QueueCounts(0, 0, 0, 0, 0), await ep.GetCountsAsync(Ct));
        foreach (var state in ListableStates)
        {
            var page = await ep.GetMessagesAsync(state, ct: Ct);
            Assert.Empty(page.Messages);
            Assert.Null(page.NextCursor);
        }
    }

    [Fact]
    public async Task Counts_SplitByState_WithoutChangingRows()
    {
        RequireSqlServer();
        await using var ep = CreateEndPoint();
        await ep.PostAsync("a"u8.ToArray(), Ct);
        await ep.PostAsync("b"u8.ToArray(), Ct);
        await ep.PostDeferredAsync("c"u8.ToArray(), DateTimeOffset.UtcNow.AddHours(1), Ct);
        await ep.GetCountsAsync(Ct); // create the endpoint's existing schema before direct state fixtures
        await ExecuteAsync($"""
            INSERT INTO {_tableName} (message_id, status, attempt_count, payload, failed_at_utc, last_error)
            VALUES (NEWID(), 3, 5, CONVERT(varbinary(max), 'f'), SYSUTCDATETIME(), N'boom');
            INSERT INTO {_tableName} (message_id, status, attempt_count, payload, completed_at_utc)
            VALUES (NEWID(), 2, 1, CONVERT(varbinary(max), 'd'), SYSUTCDATETIME());
            INSERT INTO {_tableName} (message_id, status, attempt_count, payload, locked_until_utc)
            VALUES (NEWID(), 1, 1, CONVERT(varbinary(max), 'i'), DATEADD(HOUR, 1, SYSUTCDATETIME()));
            """);

        var counts = await ep.GetCountsAsync(Ct);

        Assert.Equal(new QueueCounts(Pending: 2, InProgress: 1, Deferred: 1, Failed: 1, Completed: 1), counts);
        Assert.Equal(4, counts.Active);
        Assert.Equal(counts, await ep.GetCountsAsync(Ct));
    }

    [Fact]
    public async Task Pending_PagesInPostOrder_WithCursor()
    {
        RequireSqlServer();
        await using var ep = CreateEndPoint();
        for (var i = 0; i < 5; i++)
            await ep.PostAsync(Encoding.UTF8.GetBytes($"m{i}"), Ct);

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
        RequireSqlServer();
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
        RequireSqlServer();
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
        await ExecuteAsync($"UPDATE {_tableName}_deferred SET deliver_at_utc = DATEADD(SECOND, -1, SYSUTCDATETIME())");
        await ep.StartListeningAsync(async (_, ct) =>
        {
            await Task.Delay(System.Threading.Timeout.Infinite, ct);
            return ProcessResult.Completed;
        }, Ct);
        await WaitUntilAsync(async () => (await ep.GetCountsAsync(Ct)).Deferred == 0);
        await ep.StopListeningAsync();
        var promoted = (await ep.GetMessagesAsync(QueueInspectionState.InProgress, ct: Ct)).Messages.Concat(
            (await ep.GetMessagesAsync(QueueInspectionState.Pending, ct: Ct)).Messages);
        Assert.Contains(promoted, m => m.Id == id);
    }

    [Fact]
    public async Task Failed_HasErrorAttemptsAndTime_FiltersBySince_AndPagesWithTiedTimes()
    {
        RequireSqlServer();
        await using var ep = CreateEndPoint();
        await ep.GetCountsAsync(Ct);
        await ExecuteAsync($"""
            INSERT INTO {_tableName} (message_id, status, attempt_count, payload, failed_at_utc, last_error)
            VALUES (NEWID(), 3, 3, CONVERT(varbinary(max), 'old'), '2001-01-01T00:00:01', N'e1');
            INSERT INTO {_tableName} (message_id, status, attempt_count, payload, failed_at_utc, last_error)
            VALUES (NEWID(), 3, 2, CONVERT(varbinary(max), 'mid'), '2001-01-01T00:00:02', N'e2');
            INSERT INTO {_tableName} (message_id, status, attempt_count, payload, failed_at_utc, last_error)
            VALUES (NEWID(), 3, 1, CONVERT(varbinary(max), 'mid2'), '2001-01-01T00:00:02', N'e3');
            INSERT INTO {_tableName} (message_id, status, attempt_count, payload, failed_at_utc, last_error)
            VALUES (NEWID(), 3, 1, CONVERT(varbinary(max), 'new'), '2001-01-01T00:00:03', N'e4');
            """);

        var all = await ep.GetMessagesAsync(QueueInspectionState.Failed, ct: Ct);
        Assert.Equal(["old", "mid", "mid2", "new"], Texts(all));
        Assert.Equal("e1", all.Messages[0].LastError);
        Assert.Equal(3, all.Messages[0].Attempts);
        Assert.Equal(DateTimeOffset.Parse("2001-01-01T00:00:01Z"), all.Messages[0].FailedAt);

        var since = await ep.GetMessagesAsync(QueueInspectionState.Failed, failedSince: DateTimeOffset.Parse("2001-01-01T00:00:02Z"), ct: Ct);
        Assert.Equal(["mid", "mid2", "new"], Texts(since));

        var first = await ep.GetMessagesAsync(QueueInspectionState.Failed, 2, ct: Ct);
        var second = await ep.GetMessagesAsync(QueueInspectionState.Failed, 2, first.NextCursor, ct: Ct);
        Assert.Equal(["old", "mid"], Texts(first));
        Assert.Equal(["mid2", "new"], Texts(second));
        Assert.Null(second.NextCursor);
    }

    [Fact]
    public async Task InvalidArguments_Throw()
    {
        RequireSqlServer();
        await using var ep = CreateEndPoint();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => ep.GetMessagesAsync(QueueInspectionState.Completed, ct: Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => ep.GetMessagesAsync(QueueInspectionState.Pending, 0, ct: Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => ep.GetMessagesAsync(QueueInspectionState.Pending, IQueueInspector.MaxPageSize + 1, ct: Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => ep.GetMessagesAsync(QueueInspectionState.Pending, cursor: "abc", ct: Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => ep.GetMessagesAsync(QueueInspectionState.Failed, cursor: "5", ct: Ct));
    }

    [Fact]
    public async Task Broker_ReturnsInspector_ForSqlServer_AndNullForOtherTransports()
    {
        RequireSqlServer();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .AddSqlServerEndPoint("sql", new SqlServerSettings { ConnectionString = ConnectionString!, TableName = _tableName })
            .AddMemoryEndPoint("mem", mode: EndpointMode.WriteOnly)
            .Build();
        await using var provider = services.BuildServiceProvider();
        var broker = provider.GetRequiredService<INymBroker>();

        Assert.NotNull(broker.GetQueueInspector("sql"));
        Assert.Null(broker.GetQueueInspector("mem"));
        Assert.Throws<InvalidOperationException>(() => broker.GetQueueInspector("nope"));
    }
    private static readonly QueueInspectionState[] ListableStates =
    [
        QueueInspectionState.Pending,
        QueueInspectionState.InProgress,
        QueueInspectionState.Deferred,
        QueueInspectionState.Failed
    ];

    private static string[] Texts(QueuePage page) =>
        page.Messages.Select(m => Encoding.UTF8.GetString(m.Payload.Span)).ToArray();

    private static void RequireSqlServer()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString),
            $"Set {ConnectionStringVariable} to run SQL Server integration tests.");

    private async Task ExecuteAsync(string sql)
    {
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        await using var command = conn.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(Ct);
    }

    private async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!await condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "Condition not met in time.");
            await Task.Delay(25, Ct);
        }
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString)) return;

        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        await using var command = conn.CreateCommand();
        command.CommandText = $"DROP TABLE IF EXISTS {_tableName}_deferred; DROP TABLE IF EXISTS {_tableName};";
        await command.ExecuteNonQueryAsync(Ct);
    }
}
