using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Outbox;
using NymBroker.Core.Serialize;
using NymBroker.Core.Transform;
using NymBroker.Outbox.Sqlite;

namespace NymBroker.Tests;

/// <summary>
/// The transactional outbox end to end (#114): <see cref="IOutbox"/> writes in the application's transaction, the
/// dispatcher sends after the commit. On a SQLite file, so they always run; the store contract is in <see cref="OutboxStoreContractTests"/>.
/// </summary>
public sealed class OutboxTests : IAsyncDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly ServiceProvider _services = new ServiceCollection().BuildServiceProvider();
    private readonly string _filePath = Path.Combine(Path.GetTempPath(), $"nb_outbox_e2e_{Guid.NewGuid():N}.db");
    private readonly CapturingLogger<OutboxDispatcher> _logger = new();
    private readonly List<SqliteOutboxStore> _stores = [];

    private string ConnectionString => $"Data Source={_filePath};Pooling=False";

    [MessageName("tests.outbox.order-placed")]
    public sealed class OrderPlaced { public int OrderId { get; set; } }

    private sealed class RecordingEndPoint : IEndPoint
    {
        public EndpointMode Mode => EndpointMode.WriteOnly;
        public ConcurrentQueue<byte[]> Posts { get; } = new();
        public Func<byte[], CancellationToken, Task>? OnPost { get; init; }

        public async Task PostAsync(byte[] message, CancellationToken ct = default)
        {
            if (OnPost is not null) await OnPost(message, ct);
            Posts.Enqueue(message);
        }

        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
    }

    private sealed class FixedTransformer(byte[] output) : IOutputTransformer
    {
        public byte[]? Transform(RawMessageContext message, string destinationEndpoint) => output;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var store in _stores) await store.DisposeAsync();
        await _services.DisposeAsync();
        SqliteConnection.ClearAllPools();
        foreach (var suffix in new[] { "", "-wal", "-shm" })
            if (File.Exists(_filePath + suffix)) File.Delete(_filePath + suffix);
    }

    private (NymBrokerImpl Broker, IOutbox Outbox, OutboxDispatcher Dispatcher, RecordingEndPoint Endpoint) Create(
        RecordingEndPoint? endpoint = null, OutboxOptions? options = null)
    {
        endpoint ??= new RecordingEndPoint();
        var broker = TestBroker.Create(_services);
        broker.AddEndpoint("Out", endpoint);
        var store = NewStore();
        return (broker, new Core.Outbox.Outbox(broker, store), Dispatcher(store, broker, options), endpoint);
    }

    private SqliteOutboxStore NewStore()
    {
        var store = new SqliteOutboxStore(new SqliteOutboxSettings { ConnectionString = ConnectionString }, NullLogger<SqliteOutboxStore>.Instance);
        _stores.Add(store);
        return store;
    }

    private OutboxDispatcher Dispatcher(IOutboxStore store, NymBrokerImpl broker, OutboxOptions? options = null)
        => new(store, broker, options ?? new OutboxOptions(), _logger);

    private async Task<SqliteConnection> OpenAsync()
    {
        var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(Ct);
        await using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA busy_timeout=5000; CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY);";
        await command.ExecuteNonQueryAsync(Ct);
        return connection;
    }

    /// <summary>Places an order the way an application would: the business row and the event in one transaction.</summary>
    private async Task PlaceOrderAsync(IOutbox outbox, int orderId, bool commit = true, DateTimeOffset? deliverAt = null)
    {
        await using var connection = await OpenAsync();
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(Ct);
        await using (var insert = connection.CreateCommand())
        {
            insert.Transaction = transaction;
            insert.CommandText = $"INSERT INTO orders (id) VALUES ({orderId})";
            await insert.ExecuteNonQueryAsync(Ct);
        }

        var message = new OrderPlaced { OrderId = orderId };
        if (deliverAt is { } due) await outbox.PostDeferredAsync("Out", message, due, transaction, Ct);
        else await outbox.PostAsync("Out", message, transaction, Ct);

        if (commit) await transaction.CommitAsync(Ct);
        else await transaction.RollbackAsync(Ct);
    }

    private static int OrderId(byte[] envelope)
        => JsonDocument.Parse(envelope).RootElement.GetProperty("message").GetProperty("orderId").GetInt32();

    private static string EnvelopeId(byte[] envelope)
        => JsonDocument.Parse(envelope).RootElement.GetProperty("id").GetString()!;

    [Fact]
    public async Task Committed_IsSentByTheDispatcher_Once()
    {
        var (_, outbox, dispatcher, endpoint) = Create();

        await PlaceOrderAsync(outbox, 1);
        Assert.Empty(endpoint.Posts);   // nothing is sent by the post itself

        Assert.Equal(1, await dispatcher.DispatchBatchAsync(Ct));
        Assert.Equal(1, OrderId(Assert.Single(endpoint.Posts)));
        Assert.Equal(0, await dispatcher.DispatchBatchAsync(Ct));
        Assert.Single(endpoint.Posts);
    }

    [Fact]
    public async Task RolledBack_IsNeverSent()
    {
        var (_, outbox, dispatcher, endpoint) = Create();

        await PlaceOrderAsync(outbox, 1, commit: false);

        Assert.Equal(0, await dispatcher.DispatchBatchAsync(Ct));
        Assert.Empty(endpoint.Posts);
    }

    [Fact]
    public async Task Post_ChecksTheEndpointAndTheTransaction_Immediately()
    {
        var (broker, outbox, _, _) = Create();
        broker.AddEndpoint("ReadOnly", new RecordingEndPoint().WithMode(EndpointMode.ReadOnly));
        await using var connection = await OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync(Ct);

        await Assert.ThrowsAsync<InvalidOperationException>(() => outbox.PostAsync("Missing", new OrderPlaced(), transaction, Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => outbox.PostAsync("ReadOnly", new OrderPlaced(), transaction, Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => outbox.PostAsync("Out", new byte[] { 1 }, transaction, Ct));

        await transaction.CommitAsync(Ct);
        await Assert.ThrowsAsync<ArgumentException>(() => outbox.PostAsync("Out", new OrderPlaced(), transaction, Ct));
    }

    [Fact]
    public async Task CrashAfterTheSend_SendsAgainWithTheSameId_AndAnIdempotentReceiverDropsIt()
    {
        var (broker, outbox, _, endpoint) = Create();
        await PlaceOrderAsync(outbox, 7);

        // A dispatcher claims the row, sends it, and dies before it can remove the row.
        var store = NewStore();
        var claimed = Assert.Single(await store.ClaimAsync(Guid.NewGuid(), 10, TimeSpan.FromSeconds(1), Ct));
        await broker.SendFromOutboxAsync(claimed, Ct);
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);

        // The next dispatcher finds the expired lease and sends the message again.
        Assert.Equal(1, await Dispatcher(store, broker).DispatchBatchAsync(Ct));
        var posts = endpoint.Posts.ToArray();
        Assert.Equal(2, posts.Length);
        Assert.Equal(EnvelopeId(posts[0]), EnvelopeId(posts[1]));

        // At least once: the receiver drops the duplicate by its id.
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker().AddConsumer<CountingConsumer>().AddIdempotentReceiver().Build();
        await using var provider = services.BuildServiceProvider();
        var receiver = provider.GetRequiredService<NymBrokerImpl>();
        foreach (var post in posts)
            await receiver.ProcessAsync(post, "In", Ct);
        Assert.Equal(1, CountingConsumer.CallsFor(7));
    }

    public sealed class CountingConsumer : IConsume<OrderPlaced>
    {
        private static readonly ConcurrentDictionary<int, int> Calls = new();
        public static int CallsFor(int orderId) => Calls.GetValueOrDefault(orderId);

        public Task ConsumeAsync(OrderPlaced message, IMessageContext context, CancellationToken ct = default)
        {
            Calls.AddOrUpdate(message.OrderId, 1, (_, n) => n + 1);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task FailingSend_IsRetried_ThenLeftAsFailed()
    {
        var failing = new RecordingEndPoint { OnPost = (_, _) => throw new IOException("down") };
        var (_, outbox, dispatcher, _) = Create(failing, new OutboxOptions { MaxAttempts = 2, RetryDelay = TimeSpan.Zero, MaxRetryDelay = TimeSpan.Zero });
        await PlaceOrderAsync(outbox, 1);

        Assert.Equal(1, await dispatcher.DispatchBatchAsync(Ct));   // attempt 1: retry
        Assert.Equal(1, await dispatcher.DispatchBatchAsync(Ct));   // attempt 2: failed
        Assert.Equal(0, await dispatcher.DispatchBatchAsync(Ct));   // never again

        Assert.Single(_logger.Entries, e => e.Level == LogLevel.Warning && e.Exception is IOException);
        Assert.Single(_logger.Entries, e => e.Level == LogLevel.Error && e.Message.Contains("stays in the outbox as failed"));
        await using var connection = await OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT status || ':' || attempts || ':' || last_error FROM nymbroker_outbox";
        Assert.Equal("2:2:IOException: down", await command.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public async Task EndpointNoLongerRegistered_IsLeftAsFailed()
    {
        var (_, outbox, _, _) = Create();
        await PlaceOrderAsync(outbox, 1);

        var otherBroker = TestBroker.Create(_services);   // "Out" was removed from the configuration
        Assert.Equal(1, await Dispatcher(NewStore(), otherBroker).DispatchBatchAsync(Ct));

        Assert.Contains(_logger.Entries, e => e.Level == LogLevel.Error && e.Message.Contains("No writable endpoint named 'Out'"));
        Assert.Equal(0, await Dispatcher(NewStore(), otherBroker).DispatchBatchAsync(Ct));
    }

    [Fact]
    public async Task Deferred_WaitsInTheOutbox_UntilItIsDue()
    {
        var (_, outbox, dispatcher, endpoint) = Create();
        var deliverAt = DateTimeOffset.UtcNow.AddSeconds(1.5);
        await PlaceOrderAsync(outbox, 1, deliverAt: deliverAt);

        Assert.Equal(0, await dispatcher.DispatchBatchAsync(Ct));
        await Task.Delay(TimeSpan.FromSeconds(2), Ct);
        Assert.Equal(1, await dispatcher.DispatchBatchAsync(Ct));

        await DeferredContract.WaitUntilAsync(() => !endpoint.Posts.IsEmpty, Timeout);
        using var envelope = JsonDocument.Parse(Assert.Single(endpoint.Posts));
        Assert.Equal(deliverAt.UtcDateTime, envelope.RootElement.GetProperty("created").GetDateTime());
    }

    [Fact]
    public async Task OutputTransformer_RunsWhenTheMessageIsSent()
    {
        var (broker, outbox, dispatcher, endpoint) = Create();
        await PlaceOrderAsync(outbox, 1);
        broker.AddOutputTransformer(new FixedTransformer("<order/>"u8.ToArray()), "Out");

        await dispatcher.DispatchBatchAsync(Ct);

        Assert.Equal("<order/>"u8.ToArray(), Assert.Single(endpoint.Posts));
    }

    [Fact]
    public async Task Stopping_ReleasesWhatWasNotSent_WithoutCountingTheAttempt()
    {
        using var stop = new CancellationTokenSource();
        var hanging = new RecordingEndPoint
        {
            OnPost = async (_, ct) =>
            {
                await stop.CancelAsync();
                await Task.Delay(System.Threading.Timeout.Infinite, ct);
            }
        };
        var (_, outbox, dispatcher, _) = Create(hanging);
        for (var i = 1; i <= 3; i++) await PlaceOrderAsync(outbox, i);

        Assert.Equal(3, await dispatcher.DispatchBatchAsync(stop.Token));

        var again = await NewStore().ClaimAsync(Guid.NewGuid(), 10, TimeSpan.FromMinutes(1), Ct);
        Assert.Equal(3, again.Count);
        Assert.All(again, m => Assert.Equal(1, m.Attempt));
    }

    [Fact]
    public async Task HostedDispatcher_SendsCommittedMessages_UntilStopped()
    {
        var (_, outbox, dispatcher, endpoint) = Create(options: new OutboxOptions { PollInterval = TimeSpan.FromMilliseconds(50) });
        await dispatcher.StartAsync(Ct);
        try
        {
            await PlaceOrderAsync(outbox, 1);
            await PlaceOrderAsync(outbox, 2);
            await DeferredContract.WaitUntilAsync(() => endpoint.Posts.Count == 2, Timeout);
        }
        finally
        {
            await dispatcher.StopAsync(CancellationToken.None);
        }

        Assert.Equal([1, 2], endpoint.Posts.Select(OrderId));
        Assert.DoesNotContain(_logger.Entries, e => e.Level >= LogLevel.Error);
    }

    [Fact]
    public void Options_AreValidated_AndTheRetryDelayDoubles()
    {
        Assert.Throws<ArgumentException>(() => new OutboxOptions { BatchSize = 0 }.Validate());
        Assert.Throws<ArgumentException>(() => new OutboxOptions { LeaseTimeout = TimeSpan.FromMilliseconds(500) }.Validate());
        Assert.Throws<ArgumentException>(() => new OutboxOptions { MaxAttempts = 0 }.Validate());
        Assert.Throws<ArgumentException>(() => new OutboxOptions { PollInterval = TimeSpan.Zero }.Validate());
        Assert.Throws<ArgumentException>(() => new OutboxOptions { RetryDelay = TimeSpan.FromMinutes(10) }.Validate());

        var options = new OutboxOptions { RetryDelay = TimeSpan.FromSeconds(1), MaxRetryDelay = TimeSpan.FromSeconds(5) };
        int[] attempts = [1, 2, 3, 4, 10];
        Assert.Equal([1, 2, 4, 5, 5], attempts.Select(a => options.RetryDelayAfter(a).TotalSeconds));
    }
}

internal static class RecordingEndPointExtensions
{
    /// <summary>Only for the read-only check: the endpoint class is write-only otherwise.</summary>
    public static IEndPoint WithMode(this IEndPoint endpoint, EndpointMode mode) => new ModeOverride(endpoint, mode);

    private sealed class ModeOverride(IEndPoint inner, EndpointMode mode) : IEndPoint
    {
        public EndpointMode Mode => mode;
        public Task PostAsync(byte[] message, CancellationToken ct = default) => inner.PostAsync(message, ct);
        public IHealthCheckResult HealthCheck() => inner.HealthCheck();
    }
}
