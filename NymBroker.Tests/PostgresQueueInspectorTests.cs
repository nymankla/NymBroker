using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Endpoint.Postgres;

namespace NymBroker.Tests;

/// <summary>The read-only <see cref="IQueueInspector"/> of <see cref="PostgresEndPoint"/>.</summary>
public sealed class PostgresQueueInspectorTests : IAsyncLifetime
{
    private const string ConnectionStringVariable = "NYMBROKER_POSTGRES_CS";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);
    private readonly string _table = $"nb_inspect_{Guid.NewGuid():N}";
    private readonly List<PostgresEndPoint> _endpoints = [];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private PostgresEndPoint CreateEndPoint()
    {
        var endpoint = new PostgresEndPoint("inspect", new PostgresSettings
        {
            ConnectionString = ConnectionString!,
            TableName = _table
        }, NullLogger<PostgresEndPoint>.Instance);
        _endpoints.Add(endpoint);
        return endpoint;
    }

    [Fact]
    public async Task EmptyQueue_HasZeroCounts_AndEmptyPages()
    {
        RequirePostgres();
        var endpoint = CreateEndPoint();

        Assert.Equal(new QueueCounts(0, 0, 0, 0, 0), await endpoint.GetCountsAsync(Ct));
        foreach (var state in new[] { QueueInspectionState.Pending, QueueInspectionState.InProgress, QueueInspectionState.Deferred, QueueInspectionState.Failed })
        {
            var page = await endpoint.GetMessagesAsync(state, ct: Ct);
            Assert.Empty(page.Messages);
            Assert.Null(page.NextCursor);
        }
    }

    [Fact]
    public async Task Counts_SplitByState_AndInspectionDoesNotChangeRows()
    {
        RequirePostgres();
        var endpoint = CreateEndPoint();
        await endpoint.PostAsync("pending"u8.ToArray(), Ct);
        await endpoint.PostDeferredAsync("deferred"u8.ToArray(), DateTimeOffset.UtcNow.AddHours(1), Ct);
        var inProgressId = Guid.NewGuid();
        var failedId = Guid.NewGuid();
        var completedId = Guid.NewGuid();
        var failedAt = DateTimeOffset.UtcNow.AddMinutes(-1);
        await InsertRowAsync(inProgressId, (int)QueueMessageStatus.InProgress, 2, null, null, "in-progress");
        await InsertRowAsync(failedId, (int)QueueMessageStatus.Failed, 5, failedAt, "boom", "failed");
        await InsertRowAsync(completedId, (int)QueueMessageStatus.Completed, 1, null, null, "completed");

        Assert.Equal(new QueueCounts(Pending: 1, InProgress: 1, Deferred: 1, Failed: 1, Completed: 1), await endpoint.GetCountsAsync(Ct));
        Assert.Equal("in-progress", Encoding.UTF8.GetString(
            (await endpoint.GetMessagesAsync(QueueInspectionState.InProgress, ct: Ct)).Messages.Single().Payload.Span));
        var failed = (await endpoint.GetMessagesAsync(QueueInspectionState.Failed, ct: Ct)).Messages.Single();
        Assert.Equal(failedId.ToString(), failed.Id);
        Assert.Equal(5, failed.Attempts);
        Assert.Equal("boom", failed.LastError);
        Assert.Equal(failedAt.ToUnixTimeMilliseconds(), failed.FailedAt!.Value.ToUnixTimeMilliseconds());
        Assert.Equal(1, await ScalarAsync<long>($"SELECT COUNT(*) FROM {_table} WHERE status = {(int)QueueMessageStatus.InProgress} AND message_id = @id", inProgressId));
        Assert.Equal(new QueueCounts(Pending: 1, InProgress: 1, Deferred: 1, Failed: 1, Completed: 1), await endpoint.GetCountsAsync(Ct));
    }

    [Fact]
    public async Task PendingPagesInQueueOrder_AndMessageIdIsStableAcrossStates()
    {
        RequirePostgres();
        var endpoint = CreateEndPoint();
        for (var i = 0; i < 5; i++)
            await endpoint.PostAsync(Encoding.UTF8.GetBytes($"m{i}"), Ct);

        var first = await endpoint.GetMessagesAsync(QueueInspectionState.Pending, 2, ct: Ct);
        var second = await endpoint.GetMessagesAsync(QueueInspectionState.Pending, 2, first.NextCursor, ct: Ct);
        var third = await endpoint.GetMessagesAsync(QueueInspectionState.Pending, 2, second.NextCursor, ct: Ct);
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

        var id = first.Messages[0].Id;
        await ExecuteAsync($"UPDATE {_table} SET status = {(int)QueueMessageStatus.InProgress}, attempt_count = 1, locked_until_utc = NOW() + INTERVAL '1 hour' WHERE message_id = @id", Guid.Parse(id));
        var inProgress = (await endpoint.GetMessagesAsync(QueueInspectionState.InProgress, ct: Ct)).Messages.Single();
        Assert.Equal(id, inProgress.Id);
        await ExecuteAsync($"UPDATE {_table} SET status = {(int)QueueMessageStatus.Failed}, locked_until_utc = NULL, failed_at_utc = NOW(), last_error = 'failed' WHERE message_id = @id", Guid.Parse(id));
        var failed = (await endpoint.GetMessagesAsync(QueueInspectionState.Failed, ct: Ct)).Messages.Single();
        Assert.Equal(id, failed.Id);

        Assert.Equal(4, (await endpoint.GetMessagesAsync(QueueInspectionState.Pending, ct: Ct)).Messages.Count);
        Assert.Equal(1, await ScalarAsync<long>($"SELECT COUNT(*) FROM {_table} WHERE status = {(int)QueueMessageStatus.Failed} AND message_id = @id", Guid.Parse(id)));
    }

    [Fact]
    public async Task ExactPageSize_HasNoNextCursor()
    {
        RequirePostgres();
        var endpoint = CreateEndPoint();
        await endpoint.PostAsync("a"u8.ToArray(), Ct);
        await endpoint.PostAsync("b"u8.ToArray(), Ct);

        var page = await endpoint.GetMessagesAsync(QueueInspectionState.Pending, 2, ct: Ct);

        Assert.Equal(2, page.Messages.Count);
        Assert.Null(page.NextCursor);
    }

    [Fact]
    public async Task DeferredAndFailed_AreOldestFirst_FilterableAndKeysetPaged()
    {
        RequirePostgres();
        var endpoint = CreateEndPoint();
        var late = DateTimeOffset.UtcNow.AddHours(2);
        var soon = DateTimeOffset.UtcNow.AddHours(1);
        await endpoint.PostDeferredAsync("late"u8.ToArray(), late, Ct);
        await endpoint.PostDeferredAsync("soon"u8.ToArray(), soon, Ct);
        var deferred = await endpoint.GetMessagesAsync(QueueInspectionState.Deferred, ct: Ct);
        Assert.Equal(["soon", "late"], Texts(deferred));
        Assert.All(deferred.Messages, m => Assert.Equal(QueueInspectionState.Deferred, m.State));
        Assert.InRange(deferred.Messages[0].DeliverAt!.Value - soon, TimeSpan.FromMilliseconds(-1), TimeSpan.FromMilliseconds(1));
        var page = await endpoint.GetMessagesAsync(QueueInspectionState.Deferred, 1, ct: Ct);
        var rest = await endpoint.GetMessagesAsync(QueueInspectionState.Deferred, 1, page.NextCursor, ct: Ct);
        Assert.Equal(["soon"], Texts(page));
        Assert.Equal(["late"], Texts(rest));
        Assert.Null(rest.NextCursor);

        var failedAt = DateTimeOffset.UtcNow.AddMinutes(-5);
        await InsertRowAsync(Guid.NewGuid(), (int)QueueMessageStatus.Failed, 3, failedAt, "e1", "old");
        await InsertRowAsync(Guid.NewGuid(), (int)QueueMessageStatus.Failed, 2, failedAt, "e2", "same-time");
        await InsertRowAsync(Guid.NewGuid(), (int)QueueMessageStatus.Failed, 1, failedAt.AddMinutes(1), "e3", "new");
        var all = await endpoint.GetMessagesAsync(QueueInspectionState.Failed, ct: Ct);
        Assert.Equal(["old", "same-time", "new"], Texts(all));
        Assert.Equal("e1", all.Messages[0].LastError);
        Assert.Equal(3, all.Messages[0].Attempts);
        Assert.Equal(failedAt.ToUnixTimeMilliseconds(), all.Messages[0].FailedAt!.Value.ToUnixTimeMilliseconds());
        var since = await endpoint.GetMessagesAsync(QueueInspectionState.Failed,
            failedSince: DateTimeOffset.FromUnixTimeSeconds(failedAt.ToUnixTimeSeconds()).AddMilliseconds(500), ct: Ct);
        Assert.Equal(["old", "same-time", "new"], Texts(since));
        var p1 = await endpoint.GetMessagesAsync(QueueInspectionState.Failed, 2, ct: Ct);
        var p2 = await endpoint.GetMessagesAsync(QueueInspectionState.Failed, 2, p1.NextCursor, ct: Ct);
        Assert.Equal(["old", "same-time"], Texts(p1));
        Assert.Equal(["new"], Texts(p2));
        Assert.Null(p2.NextCursor);
    }

    [Fact]
    public async Task InvalidArguments_ThrowLikeSqlite()
    {
        RequirePostgres();
        var endpoint = CreateEndPoint();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => endpoint.GetMessagesAsync(QueueInspectionState.Completed, ct: Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => endpoint.GetMessagesAsync(QueueInspectionState.Pending, 0, ct: Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => endpoint.GetMessagesAsync(QueueInspectionState.Pending, IQueueInspector.MaxPageSize + 1, ct: Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => endpoint.GetMessagesAsync(QueueInspectionState.Pending, cursor: "abc", ct: Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => endpoint.GetMessagesAsync(QueueInspectionState.Failed, cursor: "5", ct: Ct));
    }

    private static string[] Texts(QueuePage page)
        => page.Messages.Select(m => Encoding.UTF8.GetString(m.Payload.Span)).ToArray();

    private async Task InsertRowAsync(Guid id, int status, int attempts, DateTimeOffset? failedAt, string? error, string payload)
    {
        await using var conn = new NpgsqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"INSERT INTO {_table} (message_id, status, attempt_count, failed_at_utc, last_error, payload) VALUES (@id, @status, @attempts, @failedAt, @error, @payload)";
        cmd.Parameters.Add(new NpgsqlParameter<Guid>("id", NpgsqlDbType.Uuid) { TypedValue = id });
        cmd.Parameters.Add(new NpgsqlParameter<int>("status", NpgsqlDbType.Integer) { TypedValue = status });
        cmd.Parameters.Add(new NpgsqlParameter<int>("attempts", NpgsqlDbType.Integer) { TypedValue = attempts });
        cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset?>("failedAt", NpgsqlDbType.TimestampTz) { TypedValue = failedAt });
        cmd.Parameters.Add(new NpgsqlParameter<string?>("error", NpgsqlDbType.Text) { TypedValue = error });
        cmd.Parameters.Add(new NpgsqlParameter<byte[]>("payload", NpgsqlDbType.Bytea) { TypedValue = Encoding.UTF8.GetBytes(payload) });
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static async Task ExecuteAsync(string sql, Guid id)
    {
        await using var conn = new NpgsqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.Add(new NpgsqlParameter<Guid>("id", NpgsqlDbType.Uuid) { TypedValue = id });
        await cmd.ExecuteNonQueryAsync(Ct);
    }

    private static async Task<long> ScalarAsync<T>(string sql, Guid id)
    {
        await using var conn = new NpgsqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.Add(new NpgsqlParameter<Guid>("id", NpgsqlDbType.Uuid) { TypedValue = id });
        return (long)(await cmd.ExecuteScalarAsync(Ct))!;
    }

    private static void RequirePostgres()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString),
            $"Set {ConnectionStringVariable} to run PostgreSQL integration tests.");

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var endpoint in _endpoints)
            await endpoint.DisposeAsync();
        if (!string.IsNullOrWhiteSpace(ConnectionString))
        {
            await ExecuteNoParametersAsync($"DROP TABLE IF EXISTS {_table}_deferred");
            await ExecuteNoParametersAsync($"DROP TABLE IF EXISTS {_table}");
        }
    }

    private static async Task ExecuteNoParametersAsync(string sql)
    {
        await using var conn = new NpgsqlConnection(ConnectionString);
        await conn.OpenAsync(Ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(Ct);
    }
}
