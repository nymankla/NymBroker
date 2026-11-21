using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Endpoint.Sqlite;

namespace NymBroker.Tests;

/// <summary>The failed-message contract (replay and removal) on SQLite (always runs, on a temporary database file), plus replay through the broker.</summary>
public sealed class SqliteFailedMessageTests : FailedMessageContractTests<SqliteEndPoint>
{
    private readonly SqliteTestDb.TempFile _db = new();
    private const string Table = "Replayed";

    protected override void RequireDatabase() { }

    protected override SqliteEndPoint CreateEndPoint(int maxRetryCount) => new("replay",
        new SqliteSettings { ConnectionString = _db.ConnectionString, TableName = Table, MaxRetryCount = maxRetryCount, PollInterval = TimeSpan.FromMilliseconds(10) },
        NullLogger<SqliteEndPoint>.Instance);

    protected override Task DeleteFailedRowsAsync()
        => SqliteTestDb.ExecuteAsync(_db.ConnectionString,$"PRAGMA busy_timeout = 10000; DELETE FROM {Table} WHERE Status = 3;");

    protected override Task SetFailedAtAsync(DateTimeOffset failedAt)
        => SqliteTestDb.ExecuteAsync(_db.ConnectionString, $"PRAGMA busy_timeout = 10000; UPDATE {Table} SET FailedAtUtc = {failedAt.ToUnixTimeSeconds()} WHERE Status = 3;");

    protected override ValueTask DropTableAsync()
    {
        _db.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task Broker_ReturnsAnAuditedReplayerAndRemover_ForSqlite_AndNullForOtherTransports()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .AddSqliteEndPoint("sql", new SqliteSettings { ConnectionString = _db.ConnectionString, TableName = Table })
            .AddMemoryEndPoint("mem", mode: EndpointMode.WriteOnly)
            .Build();
        await using var provider = services.BuildServiceProvider();
        var broker = provider.GetRequiredService<INymBroker>();

        Assert.IsType<AuditedQueueReplayer>(broker.GetQueueReplayer("sql"));
        Assert.Null(broker.GetQueueReplayer("mem"));
        Assert.Throws<InvalidOperationException>(() => broker.GetQueueReplayer("nope"));
        Assert.IsType<AuditedQueueFailedRemover>(broker.GetQueueFailedRemover("sql"));
        Assert.Null(broker.GetQueueFailedRemover("mem"));
        Assert.Throws<InvalidOperationException>(() => broker.GetQueueFailedRemover("nope"));
        var endpoints = broker.Describe().Endpoints.ToDictionary(e => e.Name);
        Assert.True(endpoints["sql"].Replayable);
        Assert.False(endpoints["mem"].Replayable);
        Assert.True(endpoints["sql"].FailedRemovable);
        Assert.False(endpoints["mem"].FailedRemovable);
    }

    [Fact]
    public async Task Replay_FindsALegacyTextMessageId()
    {
        var ep = EndPoint();
        await ep.GetCountsAsync(Ct);   // creates the table
        var legacyId = Guid.NewGuid().ToString().ToUpperInvariant();
        await SqliteTestDb.ExecuteAsync(_db.ConnectionString, $"""
            INSERT INTO {Table} (MessageId, Status, CreatedAtUtc, AttemptCount, Payload, FailedAtUtc, LastError)
            VALUES (CAST('{legacyId}' AS BLOB), 3, 1, 5, CAST('old' AS BLOB), 2, 'boom');
            """);
        var failed = (await ep.GetMessagesAsync(QueueInspectionState.Failed, ct: Ct)).Messages;
        Assert.Equal(legacyId, Assert.Single(failed).Id);

        var result = await ep.ReplayFailedAsync(failed, Ct);

        Assert.Equal([legacyId], result.Replayed);
        Assert.Equal("old", Encoding.UTF8.GetString(Assert.Single((await ep.GetMessagesAsync(QueueInspectionState.Pending, ct: Ct)).Messages).Payload.Span));
    }
}
