using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Factory;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;

namespace NymBroker.Tests;

/// <summary><see cref="INymBroker.PostBatchAsync{T}"/> and <see cref="INymBroker.PublishBatchAsync{T}(IEnumerable{T}, CancellationToken)"/>.</summary>
public sealed class BatchPostingTests
{
    [MessageName("tests.batch.order")]
    public sealed class BatchOrder
    {
        public int Id { get; set; }
        public string Payload { get; set; } = "";
    }

    /// <summary>Records every PostAsync / PostBatchAsync call; overrides the batch method like a transport would.</summary>
    private sealed class RecordingEndPoint(EndpointMode mode = EndpointMode.ReadWrite) : IEndPoint
    {
        public EndpointMode Mode => mode;
        public ConcurrentQueue<byte[]> Single { get; } = new();
        public ConcurrentQueue<IReadOnlyList<byte[]>> Batches { get; } = new();

        public Task PostAsync(byte[] message, CancellationToken ct = default) { Single.Enqueue(message); return Task.CompletedTask; }
        public Task PostBatchAsync(IReadOnlyList<byte[]> messages, CancellationToken ct = default) { Batches.Enqueue(messages); return Task.CompletedTask; }
        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
    }

    /// <summary>Implements only PostAsync, so it gets the interface's default PostBatchAsync.</summary>
    private sealed class SingleOnlyEndPoint : IEndPoint
    {
        public ConcurrentQueue<byte[]> Posted { get; } = new();
        public Task PostAsync(byte[] message, CancellationToken ct = default) { Posted.Enqueue(message); return Task.CompletedTask; }
        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
    }

    public sealed class RecordingConsumer : IConsume<BatchOrder>
    {
        public static readonly ConcurrentQueue<int> Received = new();
        public Task ConsumeAsync(BatchOrder message, IMessageContext context, CancellationToken ct = default)
        {
            if (message.Id == 2) throw new InvalidOperationException("order 2 is bad");
            Received.Enqueue(message.Id);
            return Task.CompletedTask;
        }
    }

    public sealed class RecordingSubscriber : ISubscribe<BatchOrder>
    {
        public static readonly ConcurrentQueue<int> Received = new();
        public Task ReceiveAsync(BatchOrder message, IMessageContext context, CancellationToken ct = default)
        {
            Received.Enqueue(message.Id);
            return Task.CompletedTask;
        }
    }

    private static (INymBroker Broker, IServiceProvider Services, MessageSerializerJson Serializer) Build(Action<NymBrokerBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var builder = services.AddNymBroker();
        configure(builder);
        builder.Build();
        var sp = services.BuildServiceProvider();
        return (sp.GetRequiredService<INymBroker>(), sp, sp.GetRequiredService<MessageSerializerJson>());
    }

    private static void AddEndPoint(NymBrokerBuilder builder, string name, IEndPoint endpoint)
    {
        builder.Services.AddKeyedSingleton(name, endpoint);
        builder.RegisterEndpoint(name);
    }

    private static List<BatchOrder> Orders(int count) => Enumerable.Range(1, count).Select(i => new BatchOrder { Id = i }).ToList();

    private static RawMessageContext Decode(MessageSerializerJson serializer, byte[] envelope)
        => (RawMessageContext)serializer.Deserialize(envelope.AsSpan());

    // --- PostBatchAsync ---

    [Fact]
    public async Task PostBatch_HandsAllEnvelopesInOrder_InOneEndpointCall()
    {
        var ep = new RecordingEndPoint();
        var (broker, _, serializer) = Build(b => AddEndPoint(b, "Out", ep));

        await broker.PostBatchAsync("Out", Orders(3), TestContext.Current.CancellationToken);

        var batch = Assert.Single(ep.Batches);
        Assert.Empty(ep.Single);
        var contexts = batch.Select(e => Decode(serializer, e)).ToList();
        Assert.Equal([1, 2, 3], contexts.Select(c => MessageSerializerJson.DeserializeMessage<BatchOrder>(c)!.Id));
        Assert.Equal(3, contexts.Select(c => c.Id).Distinct().Count());
        Assert.Equal(3, contexts.Select(c => c.CorrelationId).Distinct().Count());
        Assert.All(contexts, c => Assert.Equal("Out", c.Address?.To));
    }

    [Fact]
    public async Task PostBatch_WithCorrelationId_SetsItOnEveryEnvelope()
    {
        var ep = new RecordingEndPoint();
        var (broker, _, serializer) = Build(b => AddEndPoint(b, "Out", ep));
        var correlationId = Guid.NewGuid();

        await broker.PostBatchAsync("Out", Orders(4), TestContext.Current.CancellationToken, correlationId: correlationId);

        Assert.All(Assert.Single(ep.Batches), e => Assert.Equal(correlationId, Decode(serializer, e).CorrelationId));
    }

    [Fact]
    public async Task PostBatch_Empty_IsANoOp()
    {
        var ep = new RecordingEndPoint();
        var (broker, _, _) = Build(b => AddEndPoint(b, "Out", ep));

        await broker.PostBatchAsync("Out", Array.Empty<BatchOrder>(), TestContext.Current.CancellationToken);

        Assert.Empty(ep.Batches);
        Assert.Empty(ep.Single);
    }

    [Fact]
    public async Task PostBatch_NullElement_ThrowsBeforeSending()
    {
        var ep = new RecordingEndPoint();
        var (broker, _, _) = Build(b => AddEndPoint(b, "Out", ep));

        await Assert.ThrowsAsync<ArgumentException>(() =>
            broker.PostBatchAsync("Out", new[] { new BatchOrder { Id = 1 }, null! }, TestContext.Current.CancellationToken));

        Assert.Empty(ep.Batches);
    }

    [Fact]
    public async Task PostBatch_UnknownOrReadOnlyEndpoint_Throws()
    {
        var (broker, _, _) = Build(b => AddEndPoint(b, "ReadOnly", new RecordingEndPoint(EndpointMode.ReadOnly)));

        await Assert.ThrowsAsync<InvalidOperationException>(() => broker.PostBatchAsync("Nope", Orders(2), TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<InvalidOperationException>(() => broker.PostBatchAsync("ReadOnly", Orders(2), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task PostBatch_SplitsLargeMessagesPerMessage_IntoTheSameBatch()
    {
        var ep = new RecordingEndPoint();
        var (broker, _, serializer) = Build(b => AddEndPoint(b, "Out", ep));
        var big = new BatchOrder { Id = 1, Payload = string.Concat(Enumerable.Range(0, 200).Select(_ => Guid.NewGuid().ToString("N"))) };
        var small = new BatchOrder { Id = 2 };

        await broker.PostBatchAsync("Out", [big, small], TestContext.Current.CancellationToken, splitThresholdBytes: 1_000, compress: false);

        var batch = Assert.Single(ep.Batches);
        var types = batch.Select(e => Decode(serializer, e).MessageType).ToList();
        Assert.True(types.Count > 2);
        Assert.All(types[..^1], t => Assert.Equal(MessageTypeName.Get(typeof(SplitMessage)), t));   // the big one's parts, first
        Assert.Equal("tests.batch.order", types[^1]);                                                  // then the small one, unsplit
    }

    [Fact]
    public async Task PostBatch_EndpointWithoutOverride_GetsMessagesOneByOne_InOrder()
    {
        var ep = new SingleOnlyEndPoint();
        var (broker, _, serializer) = Build(b => AddEndPoint(b, "Out", ep));

        await broker.PostBatchAsync("Out", Orders(3), TestContext.Current.CancellationToken);

        Assert.Equal([1, 2, 3], ep.Posted.Select(e => MessageSerializerJson.DeserializeMessage<BatchOrder>(Decode(serializer, e))!.Id));
    }

    // --- Receiving side: one message at a time, settled on its own ---

    [Fact]
    public async Task PostBatch_ToMemoryEndpoint_IsConsumedOneByOne_AndOneFailureIsDeadLetteredAlone()
    {
        while (RecordingConsumer.Received.TryDequeue(out _)) { }
        var (broker, sp, _) = Build(b => b
            .AddMemoryEndPoint("Orders")
            .AddMemoryEndPoint("DLQ", mode: EndpointMode.WriteOnly)
            .WithDeadLetterEndpoint("DLQ")
            .AddConsumer<RecordingConsumer>());

        await broker.StartAsync(TestContext.Current.CancellationToken);
        await broker.PostBatchAsync("Orders", Orders(4), TestContext.Current.CancellationToken);

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (RecordingConsumer.Received.Count < 3 && DateTime.UtcNow < deadline)
            await Task.Delay(20, TestContext.Current.CancellationToken);
        await broker.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal([1, 3, 4], RecordingConsumer.Received);
        var dlq = (MemoryQueueEndPoint)sp.GetRequiredKeyedService<IEndPoint>("DLQ");
        var deadLetters = new List<string>();
        await foreach (var item in dlq.ReadAsync(TestContext.Current.CancellationToken)) deadLetters.Add(item);
        Assert.Single(deadLetters);
    }

    // --- PublishBatchAsync ---

    [Fact]
    public async Task PublishBatch_ByType_ReachesTheConsumerOneByOne()
    {
        while (RecordingConsumer.Received.TryDequeue(out _)) { }
        var (broker, _, _) = Build(b => b
            .AddMemoryEndPoint("DLQ", mode: EndpointMode.WriteOnly)
            .WithDeadLetterEndpoint("DLQ")
            .AddConsumer<RecordingConsumer>());

        await broker.PublishBatchAsync([new BatchOrder { Id = 5 }, new BatchOrder { Id = 6 }], TestContext.Current.CancellationToken);

        Assert.Equal([5, 6], RecordingConsumer.Received);
    }

    [Fact]
    public async Task PublishBatch_ToTopic_UsesOneBatchPerEndpoint_AndSubscribersReceiveOneByOne()
    {
        while (RecordingSubscriber.Received.TryDequeue(out _)) { }
        var ep = new RecordingEndPoint();
        var topicName = "orders-" + Guid.NewGuid().ToString("N");
        var (broker, _, serializer) = Build(b =>
        {
            AddEndPoint(b, "TopicOut", ep);
            b.AddTopic<BatchOrder>(topicName).SubscribeTo("TopicOut").SubscribeWith<RecordingSubscriber>().Build();
        });

        long routed = 0;
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == NymBrokerDiagnostics.InstrumentationName && instrument.Name == "nymbroker.messages.routed")
                    l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            foreach (var tag in tags)
                if (tag.Key == "topic" && Equals(tag.Value, topicName)) Interlocked.Add(ref routed, value);
        });
        listener.Start();

        await broker.PublishBatchAsync(topicName, Orders(3), TestContext.Current.CancellationToken);

        var batch = Assert.Single(ep.Batches);
        Assert.Equal([1, 2, 3], batch.Select(e => MessageSerializerJson.DeserializeMessage<BatchOrder>(Decode(serializer, e))!.Id));
        Assert.Equal([1, 2, 3], RecordingSubscriber.Received);
        Assert.Equal(3, Interlocked.Read(ref routed));
    }

    [Fact]
    public async Task PublishBatch_UnknownTopic_IsANoOp()
    {
        var (broker, _, _) = Build(_ => { });

        await broker.PublishBatchAsync("no-such-topic", Orders(2), TestContext.Current.CancellationToken);
    }
}
