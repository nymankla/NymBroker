using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using MySqlConnector;
using NymBroker.Core;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Endpoint.MySql;

namespace NymBroker.Tests;

/// <summary>Read-only queue-inspection integration tests for <see cref="MySqlEndPoint"/>.</summary>
public sealed class MySqlQueueInspectorTests : IAsyncLifetime
{
    private const string ConnectionStringVariable = "NYMBROKER_MYSQL_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);
    private readonly string _table = $"nb_inspect_{Guid.NewGuid():N}";
    private readonly List<MySqlEndPoint> _endpoints = [];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task EmptyQueue_HasZeroCounts_AndEmptyPages()
    {
        RequireMySql();
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
    public async Task Counts_SplitByState()
    {
        RequireMySql();
        await using var ep = CreateEndPoint();
        await ep.PostAsync("a"u8.ToArray(), Ct);
        await ep.PostAsync("b"u8.ToArray(), Ct);
        await ep.PostDeferredAsync("c"u8.ToArray(), DateTimeOffset.UtcNow.AddHours(1), Ct);
        await InsertQueueRowAsync(status: MySqlQueueSql.Failed, "f", attempt: 5, failedAt: DateTime.UtcNow);
        await InsertQueueRowAsync(status: MySqlQueueSql.Completed, "d", attempt: 1);
        await InsertQueueRowAsync(status: MySqlQueueSql.InProgress, "i", attempt: 1);

        var counts = await ep.GetCountsAsync(Ct);

        Assert.Equal(new QueueCounts(Pending: 2, InProgress: 1, Deferred: 1, Failed: 1, Completed: 1), counts);
        Assert.Equal(4, counts.Active);
    }

    [Fact]
    public async Task Pending_PagesInPostOrder_WithCursor()
    {
        RequireMySql();
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
        RequireMySql();
        await using var ep = CreateEndPoint();
        await ep.PostAsync("a"u8.ToArray(), Ct);
        await ep.PostAsync("b"u8.ToArray(), Ct);

        var page = await ep.GetMessagesAsync(QueueInspectionState.Pending, 2, ct: Ct);

        Assert.Equal(2, page.Messages.Count);
        Assert.Null(page.NextCursor);
    }

    [Fact]
    public async Task Deferred_ListsDeliveryTime_AndKeepsIdWhenPromoted()
    {
        RequireMySql();
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
        await ExecuteAsync($"UPDATE `{_table}_deferred` SET deliver_at_utc = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND");
        await ep.StartListeningAsync(async (_, ct) => { await Task.Delay(System.Threading.Timeout.Infinite, ct); return ProcessResult.Completed; }, Ct);
        await WaitUntilAsync(async () => (await ep.GetCountsAsync(Ct)).Deferred == 0);
        await ep.StopListeningAsync();
        var promoted = (await ep.GetMessagesAsync(QueueInspectionState.InProgress, ct: Ct)).Messages.Concat(
            (await ep.GetMessagesAsync(QueueInspectionState.Pending, ct: Ct)).Messages);
        Assert.Contains(promoted, m => m.Id == id);
    }

    [Fact]
    public async Task Failed_HasErrorAttemptsAndTime_FiltersBySince_AndPages()
    {
        RequireMySql();
        await using var ep = CreateEndPoint();
        await ep.GetCountsAsync(Ct);
        var start = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var ids = new List<Guid>();
        foreach (var (payload, attempt, seconds, error) in new[]
        {
            ("old", 3, 0, "e1"), ("mid", 2, 1, "e2"), ("mid2", 1, 1, "e3"), ("new", 1, 2, "e4")
        })
        {
            var id = Guid.NewGuid();
            ids.Add(id);
            await InsertQueueRowAsync(MySqlQueueSql.Failed, payload, attempt, id, start.AddSeconds(seconds), error);
        }

        var all = await ep.GetMessagesAsync(QueueInspectionState.Failed, ct: Ct);
        Assert.Equal(["old", "mid", "mid2", "new"], Texts(all));
        Assert.Equal(ids[0].ToString(), all.Messages[0].Id);
        Assert.Equal("e1", all.Messages[0].LastError);
        Assert.Equal(3, all.Messages[0].Attempts);
        Assert.Equal(new DateTimeOffset(start), all.Messages[0].FailedAt);

        var since = await ep.GetMessagesAsync(QueueInspectionState.Failed, failedSince: new DateTimeOffset(start.AddSeconds(1)), ct: Ct);
        Assert.Equal(["mid", "mid2", "new"], Texts(since));

        var p1 = await ep.GetMessagesAsync(QueueInspectionState.Failed, 2, ct: Ct);
        var p2 = await ep.GetMessagesAsync(QueueInspectionState.Failed, 2, p1.NextCursor, ct: Ct);
        Assert.Equal(["old", "mid"], Texts(p1));
        Assert.Equal(["mid2", "new"], Texts(p2));
    }

    [Fact]
    public async Task InvalidArguments_Throw()
    {
        RequireMySql();
        await using var ep = CreateEndPoint();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => ep.GetMessagesAsync(QueueInspectionState.Completed, ct: Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => ep.GetMessagesAsync(QueueInspectionState.Pending, 0, ct: Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => ep.GetMessagesAsync(QueueInspectionState.Pending, IQueueInspector.MaxPageSize + 1, ct: Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => ep.GetMessagesAsync(QueueInspectionState.Pending, cursor: "abc", ct: Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => ep.GetMessagesAsync(QueueInspectionState.Failed, cursor: "5", ct: Ct));
    }

    [Fact]
    public async Task Broker_ReturnsInspector_ForMySql_AndNullForOtherTransports()
    {
        RequireMySql();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .AddMySqlEndPoint("sql", new MySqlSettings { ConnectionString = ConnectionString!, TableName = _table })
            .AddMemoryEndPoint("mem", mode: EndpointMode.WriteOnly)
            .Build();
        await using var provider = services.BuildServiceProvider();
        var broker = provider.GetRequiredService<INymBroker>();
        Assert.NotNull(broker.GetQueueInspector("sql"));
        Assert.Null(broker.GetQueueInspector("mem"));
        Assert.Throws<InvalidOperationException>(() => broker.GetQueueInspector("nope"));
    }

    private static readonly QueueInspectionState[] ListableStates =
        [QueueInspectionState.Pending, QueueInspectionState.InProgress, QueueInspectionState.Deferred, QueueInspectionState.Failed];

    private MySqlEndPoint CreateEndPoint()
    {
        var endpoint = new MySqlEndPoint("Inspect", new MySqlSettings
        {
            ConnectionString = ConnectionString!,
            TableName = _table,
            PollInterval = TimeSpan.FromMilliseconds(20)
        }, NullLogger<MySqlEndPoint>.Instance);
        _endpoints.Add(endpoint);
        return endpoint;
    }

    private async Task InsertQueueRowAsync(int status, string payload, int attempt = 0, Guid? id = null,
        DateTime? failedAt = null, string? error = null)
    {
        var table = MySqlQueueSql.QuoteTableName(_table);
        await using var conn = await OpenAsync();
        await using var command = new MySqlCommand($"INSERT INTO {table} (message_id, status, attempt_count, payload, failed_at_utc, last_error) VALUES (@id, @status, @attempt, @payload, @failedAt, @error)", conn);
        command.Parameters.AddWithValue("@id", (id ?? Guid.NewGuid()).ToByteArray());
        command.Parameters.AddWithValue("@status", status);
        command.Parameters.AddWithValue("@attempt", attempt);
        command.Parameters.AddWithValue("@payload", Encoding.UTF8.GetBytes(payload));
        command.Parameters.AddWithValue("@failedAt", failedAt is null ? DBNull.Value : failedAt.Value);
        command.Parameters.AddWithValue("@error", error is null ? DBNull.Value : error);
        await command.ExecuteNonQueryAsync(Ct);
    }

    private static async Task<MySqlConnection> OpenAsync()
    {
        var conn = new MySqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        return conn;
    }

    private static async Task ExecuteAsync(string sql)
    {
        await using var conn = await OpenAsync();
        await using var command = new MySqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync(Ct);
    }

    private static string[] Texts(QueuePage page) =>
        page.Messages.Select(m => Encoding.UTF8.GetString(m.Payload.Span)).ToArray();

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!await condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "Condition not met in time.");
            await Task.Delay(20, Ct);
        }
    }

    private static void RequireMySql()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString),
            $"Set {ConnectionStringVariable} to run MySQL integration tests (scripts/setup-mysql.ps1).");

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var endpoint in _endpoints)
            await endpoint.DisposeAsync();

        if (!string.IsNullOrWhiteSpace(ConnectionString))
        {
            await ExecuteAsync($"DROP TABLE IF EXISTS `{_table}_deferred`");
            await ExecuteAsync($"DROP TABLE IF EXISTS `{_table}`");
        }
    }
}
