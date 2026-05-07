using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Sql;

namespace NymBroker.Tests;

public sealed class SqlEndPointTests : IAsyncDisposable
{
    private readonly SqlEndPoint _ep = new("test-sql",
        new SqlSettings { ConnectionString = "Data Source=:memory:", AutoCreateTable = true },
        NullLogger<SqlEndPoint>.Instance);

    public async ValueTask DisposeAsync() => await _ep.DisposeAsync();

    [Fact]
    public async Task PostAsync_InsertsRow()
    {
        var payload = """{"test":true}""";
        await _ep.PostAsync(new MemoryStream(Encoding.UTF8.GetBytes(payload)));

        var items = new List<string>();
        await foreach (var item in _ep.ReadAsync())
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
        await foreach (var item in _ep.ReadAsync())
            items.Add(item);

        Assert.Equal(3, items.Count);
    }

    [Fact]
    public async Task ReadAsync_MarksRowsProcessed()
    {
        await Post("""{"x":1}""");

        await foreach (var _ in _ep.ReadAsync()) { }

        var items = new List<string>();
        await foreach (var item in _ep.ReadAsync())
            items.Add(item);

        Assert.Empty(items);
    }

    [Fact]
    public async Task ReadAsync_IgnoresAlreadyProcessed()
    {
        await Post("""{"a":1}""");
        await Post("""{"a":2}""");

        await foreach (var _ in _ep.ReadAsync()) { }

        await Post("""{"a":3}""");

        var items = new List<string>();
        await foreach (var item in _ep.ReadAsync())
            items.Add(item);

        Assert.Single(items);
    }

    [Fact]
    public async Task ReadAsync_RespectsBatchSize()
    {
        await using var ep = new SqlEndPoint("batch-test",
            new SqlSettings { ConnectionString = "Data Source=:memory:", AutoCreateTable = true, BatchSize = 2 },
            NullLogger<SqlEndPoint>.Instance);

        for (var i = 0; i < 5; i++)
            await ep.PostAsync(new MemoryStream(Encoding.UTF8.GetBytes($$$"""{"i":{{{i}}}}""")));

        var items = new List<string>();
        await foreach (var item in ep.ReadAsync())
            items.Add(item);

        Assert.Equal(2, items.Count);
    }

    [Fact]
    public async Task ReadAsync_NoDuplicatesOnConsecutiveCalls()
    {
        await Post("""{"seq":1}""");
        await Post("""{"seq":2}""");

        var first = new List<string>();
        await foreach (var item in _ep.ReadAsync())
            first.Add(item);

        var second = new List<string>();
        await foreach (var item in _ep.ReadAsync())
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
        await using var ep = new SqlEndPoint("listen-test",
            new SqlSettings
            {
                ConnectionString = "Data Source=:memory:",
                AutoCreateTable  = true,
                PollInterval     = TimeSpan.Zero
            },
            NullLogger<SqlEndPoint>.Instance);

        await ep.PostAsync(new MemoryStream(Encoding.UTF8.GetBytes("""{"e":1}""")));
        await ep.PostAsync(new MemoryStream(Encoding.UTF8.GetBytes("""{"e":2}""")));

        var received = new List<string>();
        var tcs = new TaskCompletionSource();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await ep.StartListeningAsync(async (msg, _) =>
        {
            received.Add(msg);
            if (received.Count >= 2) tcs.TrySetResult();
            await Task.CompletedTask;
        }, cts.Token);

        await tcs.Task;
        await ep.StopListeningAsync();

        Assert.Equal(2, received.Count);
    }

    private Task Post(string payload) =>
        _ep.PostAsync(new MemoryStream(Encoding.UTF8.GetBytes(payload)));
}
