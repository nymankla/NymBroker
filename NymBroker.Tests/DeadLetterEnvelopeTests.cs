using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Consume;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.PubSub;
using NymBroker.Core.Serialize;

namespace NymBroker.Tests;

public sealed class DeadLetterEnvelopeTests
{
    private sealed class Order { public int Id { get; set; } }

    private sealed class ThrowingConsumer : IConsume<Order>
    {
        public Task ConsumeAsync(Order message, IMessageContext context, CancellationToken ct = default)
            => throw new InvalidOperationException("stock not available");
    }

    private sealed class RecordingConsumer : IConsume<Order>, IConsume<UndecodableMessage>
    {
        public List<(Order Message, DeadLetterInfo? DeadLetter)> Orders { get; } = [];
        public List<(UndecodableMessage Message, DeadLetterInfo? DeadLetter)> Undecodable { get; } = [];

        public Task ConsumeAsync(Order message, IMessageContext context, CancellationToken ct = default)
        {
            Orders.Add((message, context.DeadLetter));
            return Task.CompletedTask;
        }

        public Task ConsumeAsync(UndecodableMessage message, IMessageContext context, CancellationToken ct = default)
        {
            Undecodable.Add((message, context.DeadLetter));
            return Task.CompletedTask;
        }
    }

    private static NymBrokerImpl BuildBroker(IMessageConsumer consumer, out MemoryQueueEndPoint dlq, params Type[] messageTypes)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKeyedSingleton<IMessageConsumer>("consumer", consumer);
        var sp = services.BuildServiceProvider();

        dlq = new MemoryQueueEndPoint("DLQ");
        var broker = new NymBrokerImpl(
            new MessageSerializerJson(),
            new AggregatorImpl(NullLogger<AggregatorImpl>.Instance),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance);
        broker.AddEndpoint("DLQ", dlq);
        foreach (var t in messageTypes) broker.RegisterConsumer(t, "consumer");
        broker.SetDeadLetterEndpoint("DLQ");
        return broker;
    }

    private static string Serialize(IMessageContext context)
    {
        using var stream = new MessageSerializerJson().Serialize(context);
        return new StreamReader(stream).ReadToEnd();
    }

    private static string OrderJson(int id = 7) => Serialize(new MessageContext<Order> { Message = new Order { Id = id } });

    private static async Task<List<string>> DrainAsync(MemoryQueueEndPoint endpoint)
    {
        var items = new List<string>();
        await foreach (var item in endpoint.ReadAsync(TestContext.Current.CancellationToken)) items.Add(item);
        return items;
    }

    private static readonly DeadLetterInfo SampleInfo = new("ConsumerFailed", "boom", "System.Exception", "In",
        new DateTime(2026, 10, 5, 12, 0, 0, DateTimeKind.Utc));

    // ── Serializer ────────────────────────────────────────────────────────────

    [Fact]
    public void NormalEnvelope_HasNoDeadLetterProperty_AndRoundTripsUnchanged()
    {
        var json = OrderJson();
        Assert.DoesNotContain("deadLetter", json);

        var serializer = new MessageSerializerJson();
        var ctx = serializer.Deserialize(json);
        Assert.Null(ctx.DeadLetter);

        using var stream = serializer.Serialize(ctx);
        Assert.Equal(json, new StreamReader(stream).ReadToEnd());
    }

    [Fact]
    public void EnvelopeWithBlock_RoundTrips_AndIsExposedOnContext()
    {
        var json = Serialize(new MessageContext<Order> { Message = new Order { Id = 1 }, DeadLetter = SampleInfo });
        Assert.Contains("\"deadLetter\"", json);

        var serializer = new MessageSerializerJson();
        foreach (var ctx in new[]
                 {
                     serializer.Deserialize(json),
                     serializer.Deserialize(Encoding.UTF8.GetBytes(json).AsSpan()),
                     serializer.Deserialize(new MemoryStream(Encoding.UTF8.GetBytes(json)))
                 })
            Assert.Equal(SampleInfo, ctx.DeadLetter);

        using var stream = serializer.Serialize(serializer.Deserialize(json));
        Assert.Equal(json, new StreamReader(stream).ReadToEnd());
    }

    [Fact]
    public void Annotate_PreservesUnknownProperties_AndReplacesExistingBlock()
    {
        var raw = Encoding.UTF8.GetBytes("""{"id":"2f1b8d7e-0000-0000-0000-000000000001","custom":{"a":1},"messageType":"x","message":{"v":1}}""");
        var first = DeadLetterEnvelope.Annotate(raw, SampleInfo);
        var second = DeadLetterEnvelope.Annotate(first, SampleInfo with { Reason = "Expired", Description = "late" });

        using var doc = JsonDocument.Parse(second);
        var root = doc.RootElement;
        Assert.Equal(1, root.GetProperty("custom").GetProperty("a").GetInt32());
        Assert.Equal(1, root.GetProperty("message").GetProperty("v").GetInt32());
        Assert.Equal("Expired", root.GetProperty("deadLetter").GetProperty("reason").GetString());
        Assert.Equal("late", root.GetProperty("deadLetter").GetProperty("description").GetString());
        Assert.Equal(1, root.EnumerateObject().Count(p => p.Name == "deadLetter"));
    }

    [Fact]
    public void Annotate_CapsDescriptionLength()
    {
        var bytes = DeadLetterEnvelope.Annotate(Encoding.UTF8.GetBytes(OrderJson()), SampleInfo with { Description = new string('x', 10_000) });
        var ctx = new MessageSerializerJson().Deserialize(bytes);
        Assert.Equal(DeadLetterEnvelope.MaxDescriptionLength, ctx.DeadLetter!.Description!.Length);
    }

    [Fact]
    public void Annotate_NotAnEnvelopeButJsonObject_AnnotatesInPlace()
    {
        var bytes = DeadLetterEnvelope.Annotate("""{"id":"not-a-guid","message":1}"""u8, SampleInfo);
        using var doc = JsonDocument.Parse(bytes);
        Assert.Equal("not-a-guid", doc.RootElement.GetProperty("id").GetString());
        Assert.True(doc.RootElement.TryGetProperty("deadLetter", out _));
    }

    [Theory]
    [InlineData("plain text, not json")]
    [InlineData("[1,2,3]")]
    [InlineData("null")]
    public void Annotate_NonObjectBytes_ProduceUndecodableEnvelope(string text)
    {
        var original = Encoding.UTF8.GetBytes(text);
        var bytes = DeadLetterEnvelope.Annotate(original, SampleInfo with { Reason = "DeserializationFailed" });

        var ctx = (RawMessageContext)new MessageSerializerJson().Deserialize(bytes);
        Assert.Equal("nymbroker.undecodable", ctx.MessageType);
        Assert.Equal("DeserializationFailed", ctx.DeadLetter!.Reason);
        var payload = MessageSerializerJson.DeserializeMessage<UndecodableMessage>(ctx)!;
        Assert.Equal(original, Convert.FromBase64String(payload.PayloadBase64));
        Assert.Equal(text, payload.PayloadText);
    }

    [Fact]
    public void Annotate_InvalidUtf8_ProducesUndecodableEnvelopeWithoutText()
    {
        byte[] original = [0xFF, 0xFE, 0x00, 0x80];
        var bytes = DeadLetterEnvelope.Annotate(original, SampleInfo);

        var ctx = (RawMessageContext)new MessageSerializerJson().Deserialize(bytes);
        var payload = MessageSerializerJson.DeserializeMessage<UndecodableMessage>(ctx)!;
        Assert.Null(payload.PayloadText);
        Assert.Equal(original, Convert.FromBase64String(payload.PayloadBase64));
    }

    // ── Broker (non-native path) ──────────────────────────────────────────────

    [Fact]
    public async Task ConsumerFailure_DeadLetterEnvelopeCarriesReasonSourceAndExceptionType()
    {
        var broker = BuildBroker(new ThrowingConsumer(), out var dlq, typeof(Order));
        var json = OrderJson(5);

        await broker.ProcessAsync(json, "OrdersIn", TestContext.Current.CancellationToken);

        var item = Assert.Single(await DrainAsync(dlq));
        var ctx = (RawMessageContext)new MessageSerializerJson().Deserialize(item);
        Assert.Equal(DeadLetterReasons.ConsumerFailed, ctx.DeadLetter!.Reason);
        Assert.Equal("OrdersIn", ctx.DeadLetter.SourceEndpoint);
        Assert.Equal(typeof(InvalidOperationException).FullName, ctx.DeadLetter.ExceptionType);
        Assert.Equal("stock not available", ctx.DeadLetter.Description);
        Assert.Equal(typeof(Order).FullName, ctx.MessageType);
        Assert.Equal(5, MessageSerializerJson.DeserializeMessage<Order>(ctx)!.Id);
    }

    [Fact]
    public async Task DeserializationFailure_ArrivesAsUndecodableMessage()
    {
        var broker = BuildBroker(new ThrowingConsumer(), out var dlq, typeof(Order));

        await broker.ProcessAsync("invalid-json", "In", TestContext.Current.CancellationToken);

        var item = Assert.Single(await DrainAsync(dlq));
        var ctx = (RawMessageContext)new MessageSerializerJson().Deserialize(item);
        Assert.Equal("nymbroker.undecodable", ctx.MessageType);
        Assert.Equal(DeadLetterReasons.DeserializationFailed, ctx.DeadLetter!.Reason);
        Assert.Equal("In", ctx.DeadLetter.SourceEndpoint);
        var payload = MessageSerializerJson.DeserializeMessage<UndecodableMessage>(ctx)!;
        Assert.Equal("invalid-json", Encoding.UTF8.GetString(Convert.FromBase64String(payload.PayloadBase64)));
    }

    [Fact]
    public async Task ExpiredMessage_DeadLetterEnvelopeCarriesExpiredReason()
    {
        var broker = BuildBroker(new ThrowingConsumer(), out var dlq, typeof(Order));
        broker.SetMaxMessageAge(TimeSpan.FromMinutes(1));
        var json = Serialize(new MessageContext<Order> { Message = new Order { Id = 3 }, Created = DateTime.UtcNow.AddHours(-1) });

        await broker.ProcessAsync(json, "In", TestContext.Current.CancellationToken);

        var item = Assert.Single(await DrainAsync(dlq));
        var ctx = (RawMessageContext)new MessageSerializerJson().Deserialize(item);
        Assert.Equal(DeadLetterReasons.Expired, ctx.DeadLetter!.Reason);
        Assert.Equal(typeof(Order).FullName, ctx.MessageType);
        Assert.Equal(3, MessageSerializerJson.DeserializeMessage<Order>(ctx)!.Id);
    }

    [Fact]
    public async Task UnknownCompression_DeadLetterEnvelopeCarriesReason()
    {
        var broker = BuildBroker(new ThrowingConsumer(), out var dlq, typeof(Order));
        var part = Serialize(new MessageContext<SplitMessage>
        {
            Message = new SplitMessage { CorrelationId = Guid.NewGuid(), GroupSize = 1, Body = Convert.ToBase64String("x"u8), Compression = "lz4" }
        });

        await broker.ProcessAsync(part, "In", TestContext.Current.CancellationToken);

        var item = Assert.Single(await DrainAsync(dlq));
        var ctx = new MessageSerializerJson().Deserialize(item);
        Assert.Equal(DeadLetterReasons.UnknownCompression, ctx.DeadLetter!.Reason);
    }

    [Fact]
    public async Task TopicDeliveryFailure_DeadLetterEnvelopeCarriesReason()
    {
        var broker = BuildBroker(new RecordingConsumer(), out var dlq, typeof(Order));
        broker.AddEndpoint("Bad", new ThrowingEndPoint());
        broker.AddTopic(new TopicContext
        {
            TopicName = "orders",
            MessageType = typeof(Order),
            SubscriberEndpoints = System.Collections.Immutable.ImmutableList.Create("Bad")
        });

        await broker.ProcessAsync(OrderJson(), "In", TestContext.Current.CancellationToken);

        var item = Assert.Single(await DrainAsync(dlq));
        var ctx = new MessageSerializerJson().Deserialize(item);
        Assert.Equal(DeadLetterReasons.TopicDeliveryFailed, ctx.DeadLetter!.Reason);
    }

    private sealed class ThrowingEndPoint : IEndPoint
    {
        public EndpointMode Mode => EndpointMode.WriteOnly;
        public Task PostAsync(byte[] message, CancellationToken ct = default) => throw new IOException("down");
        public NymBroker.Core.Endpoint.HealthCheck.IHealthCheckResult HealthCheck()
            => NymBroker.Core.Endpoint.HealthCheck.HealthCheckResult.Healthy();
    }

    [Fact]
    public async Task DeadLetteredEnvelope_IsConsumedTypedByDeadLetterConsumer()
    {
        var failing = BuildBroker(new ThrowingConsumer(), out var dlq, typeof(Order));
        await failing.ProcessAsync(OrderJson(9), "OrdersIn", TestContext.Current.CancellationToken);
        var item = Assert.Single(await DrainAsync(dlq));

        var recording = new RecordingConsumer();
        var replay = BuildBroker(recording, out _, typeof(Order), typeof(UndecodableMessage));
        await replay.ProcessAsync(item, "DLQ", TestContext.Current.CancellationToken);

        var (order, deadLetter) = Assert.Single(recording.Orders);
        Assert.Equal(9, order.Id);
        Assert.Equal(DeadLetterReasons.ConsumerFailed, deadLetter!.Reason);
    }

    [Fact]
    public async Task UndecodableEnvelope_IsConsumedByUndecodableConsumer()
    {
        var failing = BuildBroker(new ThrowingConsumer(), out var dlq, typeof(Order));
        await failing.ProcessAsync("invalid-json", "In", TestContext.Current.CancellationToken);
        var item = Assert.Single(await DrainAsync(dlq));

        var recording = new RecordingConsumer();
        var replay = BuildBroker(recording, out _, typeof(Order), typeof(UndecodableMessage));
        await replay.ProcessAsync(item, "DLQ", TestContext.Current.CancellationToken);

        var (message, deadLetter) = Assert.Single(recording.Undecodable);
        Assert.Equal("invalid-json", Encoding.UTF8.GetString(Convert.FromBase64String(message.PayloadBase64)));
        Assert.Equal(DeadLetterReasons.DeserializationFailed, deadLetter!.Reason);
    }

    [Fact]
    public async Task DeadLetterBlock_SurvivesRouting()
    {
        var failing = BuildBroker(new ThrowingConsumer(), out var dlq, typeof(Order));
        await failing.ProcessAsync(OrderJson(), "OrdersIn", TestContext.Current.CancellationToken);
        var item = Assert.Single(await DrainAsync(dlq));

        var router = BuildBroker(new RecordingConsumer(), out _, typeof(Order));
        var target = new MemoryQueueEndPoint("Target");
        router.AddEndpoint("Target", target);
        router.Route<Order>().To("Target").WhenFrom("DLQ").Build();
        await router.ProcessAsync(item, "DLQ", TestContext.Current.CancellationToken);

        var routed = Assert.Single(await DrainAsync(target));
        var ctx = new MessageSerializerJson().Deserialize(routed);
        Assert.Equal(DeadLetterReasons.ConsumerFailed, ctx.DeadLetter!.Reason);
        Assert.Equal("OrdersIn", ctx.DeadLetter.SourceEndpoint);
    }

    [Fact]
    public async Task SecondFailure_ReplacesTheBlock()
    {
        var failing = BuildBroker(new ThrowingConsumer(), out var dlq, typeof(Order));
        var first = Encoding.UTF8.GetBytes(OrderJson());
        first = DeadLetterEnvelope.Annotate(first, SampleInfo with { Reason = "Expired", SourceEndpoint = "Old" });

        await failing.ProcessAsync(first, "Replay", TestContext.Current.CancellationToken);

        var item = Assert.Single(await DrainAsync(dlq));
        Assert.Equal(1, Regex(item));
        var ctx = new MessageSerializerJson().Deserialize(item);
        Assert.Equal(DeadLetterReasons.ConsumerFailed, ctx.DeadLetter!.Reason);
        Assert.Equal("Replay", ctx.DeadLetter.SourceEndpoint);

        static int Regex(string s) => System.Text.RegularExpressions.Regex.Matches(s, "\"deadLetter\"").Count;
    }

    // ── Metrics ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task DeadLettering_RecordsCounterWithReasonSourceAndMode()
    {
        var source = "dl-metrics-" + Guid.NewGuid();
        var recorded = new List<(string? Reason, string? Mode)>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == NymBrokerDiagnostics.InstrumentationName && instrument.Name == "nymbroker.messages.dead_lettered")
                    l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
        {
            string? reason = null, mode = null, src = null;
            foreach (var tag in tags)
            {
                if (tag.Key == "reason") reason = tag.Value as string;
                if (tag.Key == "mode") mode = tag.Value as string;
                if (tag.Key == "source") src = tag.Value as string;
            }
            if (src == source) lock (recorded) recorded.Add((reason, mode));
        });
        listener.Start();

        var broker = BuildBroker(new ThrowingConsumer(), out _, typeof(Order));
        await broker.ProcessAsync(OrderJson(), source, TestContext.Current.CancellationToken);

        var entry = Assert.Single(recorded);
        Assert.Equal((DeadLetterReasons.ConsumerFailed, "broker"), entry);
    }
}
