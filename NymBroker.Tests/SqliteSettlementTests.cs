using System.Text;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Endpoint.Sqlite;

namespace NymBroker.Tests;

/// <summary>How <see cref="SqliteEndPoint"/> settles each <see cref="ProcessResult"/>, and the native vs. broker dead-letter paths end to end.</summary>
public sealed class SqliteSettlementTests : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"nymbroker-settle-{Guid.NewGuid():N}.db");
    private string ConnectionString => $"Data Source={_dbPath}";

    [MessageName("tests.sqlite.settlement.order")]
    public sealed class SettleOrder { public int Id { get; set; } }

    public sealed class ThrowingOrderConsumer : IConsume<SettleOrder>
    {
        public Task ConsumeAsync(SettleOrder message, IMessageContext context, CancellationToken ct = default)
            => throw new InvalidOperationException("consumer failed");
    }

    [Fact]
    public async Task DeadLetterResult_MarksRowFailedImmediately_WithReason()
    {
        await using var ep = CreateEndPoint(maxRetryCount: 5);
        await ep.PostAsync("{}"u8.ToArray(), TestContext.Current.CancellationToken);

        var calls = 0;
        await ep.StartListeningAsync((_, _) =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(ProcessResult.DeadLetter(DeadLetterReasons.Expired, "too old"));
        }, TestContext.Current.CancellationToken);

        await WaitUntilAsync(async () => await ScalarAsync<int>("SELECT COUNT(1) FROM NymBrokerMessages WHERE Status = 3") == 1);
        await ep.StopListeningAsync();

        Assert.Equal(1, Volatile.Read(ref calls));
        Assert.Equal(1, await ScalarAsync<int>("SELECT AttemptCount FROM NymBrokerMessages"));
        Assert.Equal("Expired: too old", await ScalarAsync<string>("SELECT LastError FROM NymBrokerMessages"));
    }

    [Fact]
    public async Task RetryResult_ReturnsRowToPending_AndItIsRedelivered()
    {
        await using var ep = CreateEndPoint(maxRetryCount: 5);
        await ep.PostAsync("{}"u8.ToArray(), TestContext.Current.CancellationToken);

        var calls = 0;
        await ep.StartListeningAsync((_, _) => Task.FromResult(Interlocked.Increment(ref calls) == 1
            ? ProcessResult.Retry(description: "try later")
            : ProcessResult.Completed), TestContext.Current.CancellationToken);

        await WaitUntilAsync(async () => await ScalarAsync<int>("SELECT COUNT(1) FROM NymBrokerMessages WHERE Status = 2") == 1);
        await ep.StopListeningAsync();

        Assert.Equal(2, Volatile.Read(ref calls));
    }

    // --- End to end through the broker ---

    [Fact]
    public async Task ConsumerFailure_NativeDeadLetter_RetriesInTable_ThenFailed_AndBrokerDeadLetterEndpointStaysEmpty()
    {
        var (broker, dlq, sp) = BuildBroker(useNativeDeadLetter: true);
        await using (sp)
        {
            await broker.StartAsync(TestContext.Current.CancellationToken);
            await broker.PostAsync("Sql", new SettleOrder { Id = 1 }, TestContext.Current.CancellationToken);

            await WaitUntilAsync(async () => await ScalarAsync<int>("SELECT COUNT(1) FROM NymBrokerMessages WHERE Status = 3") == 1);
            await broker.StopAsync(TestContext.Current.CancellationToken);

            Assert.Equal(2, await ScalarAsync<int>("SELECT AttemptCount FROM NymBrokerMessages"));
            Assert.Equal("consumer failed", await ScalarAsync<string>("SELECT LastError FROM NymBrokerMessages"));
            Assert.Empty(await DrainAsync(dlq));
        }
    }

    [Fact]
    public async Task ConsumerFailure_NativeDeadLetterOff_GoesToBrokerDeadLetterEndpoint_AndRowCompletes()
    {
        var (broker, dlq, sp) = BuildBroker(useNativeDeadLetter: false);
        await using (sp)
        {
            await broker.StartAsync(TestContext.Current.CancellationToken);
            await broker.PostAsync("Sql", new SettleOrder { Id = 2 }, TestContext.Current.CancellationToken);

            await WaitUntilAsync(async () => await ScalarAsync<int>("SELECT COUNT(1) FROM NymBrokerMessages WHERE Status = 2") == 1);
            await broker.StopAsync(TestContext.Current.CancellationToken);

            Assert.Single(await DrainAsync(dlq));
            Assert.Equal(1, await ScalarAsync<int>("SELECT AttemptCount FROM NymBrokerMessages"));
        }
    }

    // --- PostBatchAsync: one transaction, order preserved ---

    [Fact]
    public async Task PostBatch_InsertsAllRows_InOrder()
    {
        await using var ep = CreateEndPoint(maxRetryCount: 5);
        var sent = new[] { "first", "second", "third" };

        await ep.PostBatchAsync(sent.Select(s => Encoding.UTF8.GetBytes(s)).ToList(), TestContext.Current.CancellationToken);

        var read = new List<string>();
        await foreach (var item in ep.ReadAsync(TestContext.Current.CancellationToken)) read.Add(item);
        Assert.Equal(sent, read);
    }

    [Fact]
    public async Task PostBatch_IsAtomic_AFailingRowInsertsNothing()
    {
        await using var ep = CreateEndPoint(maxRetryCount: 5);
        await ep.PostAsync("setup"u8.ToArray(), TestContext.Current.CancellationToken);   // creates the table
        await using (var conn = new SqliteConnection(ConnectionString))
        {
            await conn.OpenAsync(TestContext.Current.CancellationToken);
            await SqliteTestDb.ExecuteAsync(conn, "DELETE FROM NymBrokerMessages");
            await SqliteTestDb.ExecuteAsync(conn, "CREATE TRIGGER no_boom BEFORE INSERT ON NymBrokerMessages WHEN CAST(NEW.Payload AS TEXT) = 'boom' BEGIN SELECT RAISE(ABORT, 'boom'); END");
        }

        await Assert.ThrowsAnyAsync<Exception>(() => ep.PostBatchAsync(
            ["ok-1"u8.ToArray(), "boom"u8.ToArray(), "ok-2"u8.ToArray()], TestContext.Current.CancellationToken));

        Assert.Equal(0, await ScalarAsync<int>("SELECT COUNT(1) FROM NymBrokerMessages"));
    }

    // --- helpers ---

    private SqliteEndPoint CreateEndPoint(int maxRetryCount) => new("settle", new SqliteSettings
    {
        ConnectionString = ConnectionString,
        AutoCreateTable  = true,
        PollInterval     = TimeSpan.FromMilliseconds(10),
        MaxRetryCount    = maxRetryCount,
        LeaseTimeout     = TimeSpan.FromSeconds(1)
    }, NullLogger<SqliteEndPoint>.Instance);

    private (INymBroker Broker, MemoryQueueEndPoint DeadLetters, ServiceProvider Services) BuildBroker(bool useNativeDeadLetter)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .AddSqliteEndPoint("Sql", new SqliteSettings
            {
                ConnectionString    = ConnectionString,
                AutoCreateTable     = true,
                PollInterval        = TimeSpan.FromMilliseconds(10),
                MaxRetryCount       = 2,
                UseNativeDeadLetter = useNativeDeadLetter
            })
            // WriteOnly: otherwise the started broker would also listen on the DLQ and consume what lands there.
            .AddMemoryEndPoint("DLQ", mode: EndpointMode.WriteOnly)
            .WithDeadLetterEndpoint("DLQ")
            .AddConsumer<ThrowingOrderConsumer>()
            .Build();
        var sp = services.BuildServiceProvider();
        return (sp.GetRequiredService<INymBroker>(), (MemoryQueueEndPoint)sp.GetRequiredKeyedService<IEndPoint>("DLQ"), sp);
    }

    private static async Task<List<string>> DrainAsync(MemoryQueueEndPoint ep)
    {
        var items = new List<string>();
        await foreach (var item in ep.ReadAsync(TestContext.Current.CancellationToken)) items.Add(item);
        return items;
    }

    private Task<T> ScalarAsync<T>(string sql) => SqliteTestDb.ScalarAsync<T>(ConnectionString, sql);

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Condition was not met in time.");
            await Task.Delay(20, TestContext.Current.CancellationToken);
        }
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        foreach (var file in new[] { _dbPath, _dbPath + "-wal", _dbPath + "-shm" })
        {
            try { File.Delete(file); }
            catch (IOException) { /* best effort: the listener may still hold the file briefly */ }
            catch (UnauthorizedAccessException) { /* best effort */ }
        }
    }
}
