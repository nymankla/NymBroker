using System.Text;
using Dapper;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Sql;

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
        await _ep.PostAsync(Encoding.UTF8.GetBytes(payload));

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
        await using var ep = new SqliteEndPoint("batch-test",
            new SqliteSettings { ConnectionString = "Data Source=:memory:", AutoCreateTable = true, BatchSize = 2 },
            NullLogger<SqliteEndPoint>.Instance);

        for (var i = 0; i < 5; i++)
            await ep.PostAsync(Encoding.UTF8.GetBytes($$$"""{"i":{{{i}}}}"""));

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
        await using var ep = new SqliteEndPoint("listen-test",
            new SqliteSettings
            {
                ConnectionString = "Data Source=:memory:",
                AutoCreateTable  = true,
                PollInterval     = TimeSpan.Zero,
                MaxRetryCount    = 3
            },
            NullLogger<SqliteEndPoint>.Instance);

        await ep.PostAsync(Encoding.UTF8.GetBytes("""{"e":1}"""));
        await ep.PostAsync(Encoding.UTF8.GetBytes("""{"e":2}"""));

        var received = new List<string>();
        var tcs = new TaskCompletionSource();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await ep.StartListeningAsync(async (msg, _) =>
        {
            received.Add(System.Text.Encoding.UTF8.GetString(msg));
            if (received.Count >= 2) tcs.TrySetResult();
            await Task.CompletedTask;
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

        await ep.PostAsync(Encoding.UTF8.GetBytes("""{"retry":true}"""));

        var attempts = 0;
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await ep.StartListeningAsync((_, _) =>
        {
            attempts++;
            if (attempts < 2)
                throw new InvalidOperationException("Transient failure");

            completed.TrySetResult();
            return Task.CompletedTask;
        }, cts.Token);

        await completed.Task;
        await ep.StopListeningAsync();

        Assert.Equal(2, attempts);

        var items = new List<string>();
        await foreach (var item in ep.ReadAsync())
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

            await ep.PostAsync(Encoding.UTF8.GetBytes("""{"fail":true}"""));

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
            await conn.OpenAsync();

            var failedCount = await conn.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM NymBrokerMessages WHERE Status = 3");
            var pendingCount = await conn.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM NymBrokerMessages WHERE Status = 0");
            var inProgressCount = await conn.ExecuteScalarAsync<int>("SELECT COUNT(1) FROM NymBrokerMessages WHERE Status = 1");

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

    private Task Post(string payload) =>
        _ep.PostAsync(Encoding.UTF8.GetBytes(payload));
}
