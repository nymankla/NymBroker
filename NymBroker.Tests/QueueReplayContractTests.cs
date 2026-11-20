using System.Collections.Concurrent;
using System.Text;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.Queue;

namespace NymBroker.Tests;

/// <summary>
/// The <see cref="IQueueReplayer"/> contract, run against each SQL queue endpoint. A subclass supplies the endpoint, two
/// statements that act on the table directly (as retention or another process would), skips when the database is not
/// available, and drops its table.
/// </summary>
public abstract class QueueReplayContractTests<TEndPoint> : IAsyncLifetime
    where TEndPoint : IEndPointEventDriven, IQueueInspector, IQueueReplayer, IAsyncDisposable
{
    protected static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private readonly List<TEndPoint> _endpoints = [];

    /// <summary>Skips the test unless the database is available.</summary>
    protected abstract void RequireDatabase();

    /// <summary>A new endpoint on this test's table, polling fast.</summary>
    protected abstract TEndPoint CreateEndPoint(int maxRetryCount);

    /// <summary>Deletes every Failed row, as the retention cleanup does.</summary>
    protected abstract Task DeleteFailedRowsAsync();

    /// <summary>Sets the failure time of every Failed row, as if each had failed again at <paramref name="failedAt"/>.</summary>
    protected abstract Task SetFailedAtAsync(DateTimeOffset failedAt);

    /// <summary>Drops the table (or deletes the database file).</summary>
    protected abstract ValueTask DropTableAsync();

    protected TEndPoint EndPoint(int maxRetryCount = 5)
    {
        var endpoint = CreateEndPoint(maxRetryCount);
        _endpoints.Add(endpoint);
        return endpoint;
    }

    [Fact]
    public async Task Replay_MovesOnlyTheSelectedRowsBackToPending_AndKeepsTheirEnvelope()
    {
        RequireDatabase();
        var ep = EndPoint();
        var failed = await FailAsync(ep, "a", "b", "c");
        var selected = failed.Where(m => Text(m) != "c").ToList();

        var result = await ep.ReplayFailedAsync(selected, Ct);

        Assert.Equal(selected.Select(m => m.Id), result.Replayed);
        Assert.Empty(result.Skipped);
        var counts = await ep.GetCountsAsync(Ct);
        Assert.Equal((2, 1), (counts.Pending, counts.Failed));
        var pending = (await ep.GetMessagesAsync(QueueInspectionState.Pending, ct: Ct)).Messages;
        Assert.Equal(selected.Select(m => (m.Id, Text(m))), pending.Select(m => (m.Id, Text(m))));
        Assert.All(pending, m =>
        {
            Assert.Equal(0, m.Attempts);
            Assert.StartsWith(DeadLetterReasons.ConsumerFailed, m.LastError);   // kept until the next claim
        });
        Assert.Equal(["c"], (await ep.GetMessagesAsync(QueueInspectionState.Failed, ct: Ct)).Messages.Select(Text));

        var handled = new ConcurrentBag<string>();
        await ep.StartListeningAsync((raw, _) =>
        {
            handled.Add(Encoding.UTF8.GetString(raw));
            return Task.FromResult(ProcessResult.Completed);
        }, Ct);
        await TestWait.UntilAsync(async () => (await ep.GetCountsAsync(Ct)).Completed == 2, Timeout);
        Assert.Equal(["a", "b"], handled.Order());
        Assert.Equal(1, (await ep.GetCountsAsync(Ct)).Failed);
    }

    [Fact]
    public async Task ReplayedMessage_GetsAllItsRetriesAgain()
    {
        RequireDatabase();
        var ep = EndPoint(maxRetryCount: 3);
        await ep.PostAsync("r"u8.ToArray(), Ct);
        var attempts = 0;
        await ep.StartListeningAsync((_, _) =>
        {
            Interlocked.Increment(ref attempts);
            return Task.FromResult(ProcessResult.Retry(new InvalidOperationException("still broken")));
        }, Ct);
        var failed = await WaitForFailedAsync(ep, 1);
        Assert.Equal(3, failed[0].Attempts);

        Assert.Single((await ep.ReplayFailedAsync(failed, Ct)).Replayed);

        await TestWait.UntilAsync(() => Task.FromResult(Volatile.Read(ref attempts) == 6), Timeout);
        var again = await WaitForFailedAsync(ep, 1);
        Assert.Equal(3, again[0].Attempts);
        Assert.Equal(failed[0].Id, again[0].Id);
    }

    [Fact]
    public async Task StaleSelection_IsSkipped()
    {
        RequireDatabase();
        var ep = EndPoint();
        var failed = await FailAsync(ep, "a", "b");

        Assert.Single((await ep.ReplayFailedAsync([failed[0]], Ct)).Replayed);
        var second = await ep.ReplayFailedAsync(failed, Ct);
        Assert.Equal([failed[1].Id], second.Replayed);
        Assert.Equal([failed[0].Id], second.Skipped);   // already pending

        // Fails again later: the old selection no longer matches its failure time.
        var again = await FailAsync(ep);
        Assert.Equal(2, again.Count);
        await SetFailedAtAsync(failed[0].FailedAt!.Value.AddHours(1));
        var stale = await ep.ReplayFailedAsync(failed, Ct);
        Assert.Empty(stale.Replayed);
        Assert.Equal(failed.Select(m => m.Id), stale.Skipped);
        Assert.Equal(2, (await ep.GetCountsAsync(Ct)).Failed);
    }

    [Fact]
    public async Task MixedSelection_ReportsEachMessage()
    {
        RequireDatabase();
        var ep = EndPoint();
        var failed = await FailAsync(ep, "a", "b");
        await ep.ReplayFailedAsync([failed[0]], Ct);
        var unknown = failed[1] with { Id = Guid.NewGuid().ToString() };

        var result = await ep.ReplayFailedAsync([failed[0], unknown, failed[1]], Ct);

        Assert.Equal([failed[1].Id], result.Replayed);
        Assert.Equal([failed[0].Id, unknown.Id], result.Skipped);
        Assert.Equal(2, (await ep.GetCountsAsync(Ct)).Pending);
    }

    [Fact]
    public async Task ConcurrentReplays_ReplayEachRowOnce()
    {
        RequireDatabase();
        var ep = EndPoint();
        var failed = await FailAsync(ep, [.. Enumerable.Range(0, 20).Select(i => $"m{i}")]);
        var other = EndPoint();

        var results = await Task.WhenAll(
            Task.Run(() => ep.ReplayFailedAsync(failed, Ct), Ct),
            Task.Run(() => other.ReplayFailedAsync(failed, Ct), Ct));

        var replayed = results.SelectMany(r => r.Replayed).ToList();
        Assert.Equal(failed.Select(m => m.Id).Order(), replayed.Order());
        Assert.All(results, r => Assert.Equal(20, r.Replayed.Count + r.Skipped.Count));
        Assert.Equal(20, (await ep.GetCountsAsync(Ct)).Pending);
    }

    [Fact]
    public async Task ReplayRacingRetention_EndsWithEachRowEitherPendingOrDeleted()
    {
        RequireDatabase();
        var ep = EndPoint();
        var failed = await FailAsync(ep, [.. Enumerable.Range(0, 20).Select(i => $"m{i}")]);

        var replay = Task.Run(() => ep.ReplayFailedAsync(failed, Ct), Ct);
        var delete = Task.Run(DeleteFailedRowsAsync, Ct);
        await Task.WhenAll(replay, delete);
        var result = await replay;

        Assert.Equal(20, result.Replayed.Count + result.Skipped.Count);
        var counts = await ep.GetCountsAsync(Ct);
        Assert.Equal((result.Replayed.Count, 0L), (counts.Pending, counts.Failed));
        var pending = (await ep.GetMessagesAsync(QueueInspectionState.Pending, ct: Ct)).Messages.Select(m => m.Id);
        Assert.Equal(result.Replayed.Order(), pending.Order());
    }

    [Fact]
    public async Task InvalidSelections_Throw()
    {
        RequireDatabase();
        var ep = EndPoint();
        var failed = new QueueMessageInfo(Guid.NewGuid().ToString(), QueueInspectionState.Failed, 1, DateTimeOffset.UtcNow,
            null, DateTimeOffset.UtcNow, "e", ReadOnlyMemory<byte>.Empty);

        await Assert.ThrowsAsync<ArgumentNullException>(() => ep.ReplayFailedAsync(null!, Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => ep.ReplayFailedAsync([], Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ep.ReplayFailedAsync([.. Enumerable.Repeat(failed, IQueueInspector.MaxPageSize + 1)], Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => ep.ReplayFailedAsync([failed with { State = QueueInspectionState.Pending }], Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => ep.ReplayFailedAsync([failed with { FailedAt = null }], Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => ep.ReplayFailedAsync([failed with { Id = "not-a-guid" }], Ct));
    }

    /// <summary>
    /// Posts <paramref name="texts"/>, then dead-letters every pending message through a listener on a second endpoint
    /// instance (so <paramref name="ep"/> is free to listen later), and returns all failed messages in failure order.
    /// </summary>
    protected async Task<IReadOnlyList<QueueMessageInfo>> FailAsync(TEndPoint ep, params string[] texts)
    {
        foreach (var text in texts)
            await ep.PostAsync(Encoding.UTF8.GetBytes(text), Ct);
        var counts = await ep.GetCountsAsync(Ct);
        var listener = EndPoint();
        await listener.StartListeningAsync((_, _) =>
            Task.FromResult(ProcessResult.DeadLetter(DeadLetterReasons.ConsumerFailed, "broken")), Ct);
        var failed = await WaitForFailedAsync(ep, (int)(counts.Failed + counts.Pending));
        await listener.StopListeningAsync();
        return failed;
    }

    private static async Task<IReadOnlyList<QueueMessageInfo>> WaitForFailedAsync(TEndPoint ep, int count)
    {
        IReadOnlyList<QueueMessageInfo> failed = [];
        await TestWait.UntilAsync(async () =>
        {
            failed = (await ep.GetMessagesAsync(QueueInspectionState.Failed, ct: Ct)).Messages;
            return failed.Count == count;
        }, Timeout);
        return failed;
    }

    private static string Text(QueueMessageInfo message) => Encoding.UTF8.GetString(message.Payload.Span);

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var endpoint in _endpoints)
            await endpoint.DisposeAsync();
        await DropTableAsync();
    }
}
