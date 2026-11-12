using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using NymBroker.Core;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;
using NymBroker.Endpoint.Sqlite;

namespace NymBroker.Tests;

/// <summary>
/// An <see cref="ActivityListener"/> is process-wide: while one listens, every broker in the test run creates activities
/// and writes trace context into its envelopes. Tests that listen run in this collection, alone.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ActivityListenerCollection
{
    public const string Name = "Activity listeners";
}

/// <summary>W3C trace context across message boundaries (#116).</summary>
[Collection(ActivityListenerCollection.Name)]
public sealed class TracePropagationTests : IAsyncDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private const string RemoteParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    private static readonly ActivityTraceId RemoteTraceId = ActivityTraceId.CreateFromString("0af7651916cd43dd8448eb211c80319c");
    private static readonly ActivitySpanId RemoteSpanId = ActivitySpanId.CreateFromString("b7ad6b7169203331");

    private readonly ServiceProvider _services = new ServiceCollection().AddLogging().BuildServiceProvider();
    private readonly string _filePath = Path.Combine(Path.GetTempPath(), $"nb_trace_{Guid.NewGuid():N}.db");

    [MessageName("tests.tracing.order")]
    public sealed class Order { public int Id { get; set; } public string Text { get; set; } = ""; }

    /// <summary>Collects the NymBroker activities that stop while it exists.</summary>
    private sealed class Collector : IDisposable
    {
        private readonly ActivityListener _listener;
        public ConcurrentQueue<Activity> Stopped { get; } = new();

        public Collector()
        {
            _listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == NymBrokerActivitySource.InstrumentationName,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = Stopped.Enqueue
            };
            ActivitySource.AddActivityListener(_listener);
        }

        public Activity[] Named(string name) => [.. Stopped.Where(a => a.OperationName == name)];

        public void Dispose() => _listener.Dispose();
    }

    private sealed class RecordingEndPoint : IEndPoint
    {
        public EndpointMode Mode => EndpointMode.WriteOnly;
        public ConcurrentQueue<byte[]> Posts { get; } = new();
        public Task PostAsync(byte[] message, CancellationToken ct = default) { Posts.Enqueue(message); return Task.CompletedTask; }
        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
    }

    public async ValueTask DisposeAsync()
    {
        await _services.DisposeAsync();
        SqliteConnection.ClearAllPools();
        foreach (var suffix in new[] { "", "-wal", "-shm" })
            if (File.Exists(_filePath + suffix)) File.Delete(_filePath + suffix);
    }

    private NymBrokerImpl Broker(params (string Name, IEndPoint Endpoint)[] endpoints)
    {
        var broker = TestBroker.Create(_services);
        foreach (var (name, endpoint) in endpoints) broker.AddEndpoint(name, endpoint);
        return broker;
    }

    private static string? Field(byte[] envelope, string name)
        => JsonDocument.Parse(envelope).RootElement.TryGetProperty(name, out var value) ? value.GetString() : null;

    private static byte[] Envelope(string? traceParent, string? traceState = null)
    {
        var context = new MessageContext<Order> { Message = new Order { Id = 1 }, TraceParent = traceParent, TraceState = traceState };
        return MessageSerializerJson.SerializeToBytes(context);
    }

    // --- Envelope ---

    [Fact]
    public void Envelope_CarriesTheFieldsOnlyWhenSet_AndOlderEnvelopesStillRead()
    {
        var plain = Envelope(null);
        Assert.DoesNotContain("trace", Encoding.UTF8.GetString(plain), StringComparison.OrdinalIgnoreCase);   // byte-for-byte as before
        var read = (RawMessageContext)new MessageSerializerJson().Deserialize(plain.AsSpan());
        Assert.Null(read.TraceParent);
        Assert.Null(read.TraceState);

        var traced = (RawMessageContext)new MessageSerializerJson().Deserialize(Envelope(RemoteParent, "vendor=x").AsSpan());
        Assert.Equal(RemoteParent, traced.TraceParent);
        Assert.Equal("vendor=x", traced.TraceState);
        Assert.Equal(RemoteParent, ((IMessageContext)traced).TraceParent);
    }

    [Theory]
    [InlineData("garbage", "vendor=x")]
    [InlineData("00-00000000000000000000000000000000-0000000000000000-01", null)]   // all-zero ids are invalid
    [InlineData(null, "vendor=x")]                                                  // trace state without a parent
    public void Envelope_MalformedContextIsDropped(string? traceParent, string? traceState)
    {
        var json = $$"""{"id":"{{Guid.NewGuid()}}","messageType":"x","message":{},"traceParent":{{JsonSerializer.Serialize(traceParent)}},"traceState":{{JsonSerializer.Serialize(traceState)}}}""";
        var read = (RawMessageContext)new MessageSerializerJson().Deserialize(json);
        Assert.Null(read.TraceParent);
        Assert.Null(read.TraceState);
    }

    [Fact]
    public void Envelope_OversizedTraceStateIsDropped_TheParentKept()
    {
        var read = (RawMessageContext)new MessageSerializerJson().Deserialize(Envelope(RemoteParent, "k=" + new string('v', 600)).AsSpan());
        Assert.Equal(RemoteParent, read.TraceParent);
        Assert.Null(read.TraceState);
    }

    // --- Producer ---

    [Fact]
    public async Task Post_IsASendActivity_UnderTheCaller_AndTheEnvelopeCarriesIt()
    {
        using var collector = new Collector();
        var endpoint = new RecordingEndPoint();
        var broker = Broker(("Out", endpoint));

        using (var request = new Activity("request").Start())
        {
            await broker.PostAsync("Out", new Order { Id = 7 }, Ct);
            var send = Assert.Single(collector.Named("nymbroker.send"));
            Assert.Equal(ActivityKind.Producer, send.Kind);
            Assert.Equal(request.SpanId, send.ParentSpanId);
            Assert.Equal("Out", send.GetTagItem("messaging.destination.name"));
            Assert.Equal("send", send.GetTagItem("messaging.operation.type"));
            Assert.Equal(send.Id, Field(Assert.Single(endpoint.Posts), "traceParent"));
        }
    }

    [Fact]
    public async Task Batch_IsOneSendActivity_AndEveryEnvelopeCarriesIt()
    {
        using var collector = new Collector();
        var endpoint = new RecordingEndPoint();

        await Broker(("Out", endpoint)).PostBatchAsync("Out", [new Order(), new Order(), new Order()], Ct);

        var send = Assert.Single(collector.Named("nymbroker.send"));
        Assert.Equal(3, send.GetTagItem("messaging.batch.message_count"));
        Assert.All(endpoint.Posts, p => Assert.Equal(send.Id, Field(p, "traceParent")));
    }

    [Fact]
    public async Task SplitParts_CarryTheSendActivity_AndTheReassembledMessageContinuesIt()
    {
        using var collector = new Collector();
        var endpoint = new RecordingEndPoint();
        var producer = Broker(("Out", endpoint));
        await producer.PostAsync("Out", new Order { Text = new string('x', 4000) }, Ct, splitThresholdBytes: 500, compress: false);

        var send = Assert.Single(collector.Named("nymbroker.send"));
        Assert.True(endpoint.Posts.Count > 1);
        Assert.All(endpoint.Posts, p => Assert.Equal(send.Id, Field(p, "traceParent")));

        var consumer = Broker();
        foreach (var part in endpoint.Posts) await consumer.ProcessAsync(part, "In", Ct);
        Assert.All(collector.Named("nymbroker.process"), p => Assert.Equal(send.SpanId, p.ParentSpanId));
    }

    [Fact]
    public async Task PropagationOff_WritesNoContext()
    {
        using var collector = new Collector();
        var endpoint = new RecordingEndPoint();
        var broker = Broker(("Out", endpoint));
        broker.ConfigureTracing(new TracingOptions { PropagateTraceContext = false });

        using (new Activity("request").Start())
            await broker.PostAsync("Out", new Order(), Ct);

        Assert.Null(Field(Assert.Single(endpoint.Posts), "traceParent"));
        Assert.Single(collector.Named("nymbroker.send"));   // the activity itself is still recorded
    }

    // --- Consumer ---

    public sealed class TracedConsumer : IConsume<Order>
    {
        public static readonly ConcurrentDictionary<int, ActivityContext> Seen = new();

        public Task ConsumeAsync(Order message, IMessageContext context, CancellationToken ct = default)
        {
            Seen[message.Id] = Activity.Current?.Context ?? default;
            return Task.CompletedTask;
        }
    }

    /// <summary>A producer and a consumer that share nothing but a SQLite queue file: what two processes would.</summary>
    [Theory]
    [InlineData(TraceContinuation.Parent)]
    [InlineData(TraceContinuation.Link)]
    public async Task CrossProcess_TheConsumerContinuesTheProducersTrace(TraceContinuation continuation)
    {
        using var collector = new Collector();
        var orderId = Random.Shared.Next();
        var queue = new SqliteSettings { ConnectionString = $"Data Source={_filePath}", TableName = "orders", PollInterval = TimeSpan.FromMilliseconds(20) };

        await using var consumerApp = BuildApp(b => b.AddSqliteEndPoint("Orders", queue).AddConsumer<TracedConsumer>()
            .ConfigureTracing(o => o.Continuation = continuation));
        await using var producerApp = BuildApp(b => b.AddSqliteEndPoint("Orders", queue, EndpointMode.WriteOnly));
        var consumer = consumerApp.GetRequiredService<INymBroker>();
        await consumer.StartAsync(Ct);   // outside any activity: its listener has no ambient trace

        ActivityTraceId producerTrace;
        using (var request = new Activity("http-request").Start())
        {
            producerTrace = request.TraceId;
            await producerApp.GetRequiredService<INymBroker>().PostAsync("Orders", new Order { Id = orderId }, Ct);
        }
        await DeferredContract.WaitUntilAsync(() => TracedConsumer.Seen.ContainsKey(orderId), Timeout);
        await consumer.StopAsync(Ct);

        var send = Assert.Single(collector.Named("nymbroker.send"));
        var process = Assert.Single(collector.Named("nymbroker.process"), a => a.TraceId == TracedConsumer.Seen[orderId].TraceId);
        Assert.Equal(ActivityKind.Consumer, process.Kind);
        if (continuation == TraceContinuation.Parent)
        {
            Assert.Equal(producerTrace, process.TraceId);
            Assert.Equal(send.SpanId, process.ParentSpanId);
            Assert.True(process.HasRemoteParent);
        }
        else
        {
            Assert.NotEqual(producerTrace, process.TraceId);
            Assert.Equal(default, process.ParentSpanId);
            var link = Assert.Single(process.Links);
            Assert.Equal(send.SpanId, link.Context.SpanId);
            Assert.Equal(producerTrace, link.Context.TraceId);
        }
    }

    private static ServiceProvider BuildApp(Func<Core.Factory.NymBrokerBuilder, Core.Factory.NymBrokerBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        configure(services.AddNymBroker()).Build();
        return services.BuildServiceProvider();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("00-not-a-trace-parent")]
    public async Task MissingOrInvalidContext_StartsANewTrace_AndTheMessageIsProcessed(string? traceParent)
    {
        using var collector = new Collector();

        var result = await Broker().ProcessAsync(Envelope(traceParent), "In", Ct);

        Assert.Equal(ProcessOutcome.Completed, result.Outcome);
        var process = Assert.Single(collector.Named("nymbroker.process"));
        Assert.Null(process.ParentId);
        Assert.False(process.HasRemoteParent);
    }

    [Fact]
    public async Task Redelivery_IsAnotherProcessActivity_UnderTheSameProducer()
    {
        using var collector = new Collector();
        var broker = Broker();
        var envelope = Envelope(RemoteParent);

        await broker.ProcessAsync(envelope, "In", Ct);
        await broker.ProcessAsync(envelope, "In", Ct);

        var processes = collector.Named("nymbroker.process");
        Assert.Equal(2, processes.Length);
        Assert.All(processes, p => Assert.Equal(RemoteSpanId, p.ParentSpanId));
        Assert.NotEqual(processes[0].SpanId, processes[1].SpanId);
    }

    // --- Forwarding ---

    [Fact]
    public async Task Route_ForwardsHopByHop()
    {
        using var collector = new Collector();
        var endpoint = new RecordingEndPoint();
        var broker = Broker(("Out", endpoint));
        broker.Route<Order>().To("Out").Build();

        await broker.ProcessAsync(Envelope(RemoteParent, "vendor=x"), "In", Ct);

        var process = Assert.Single(collector.Named("nymbroker.process"));
        var send = Assert.Single(collector.Named("nymbroker.send"));
        Assert.Equal(RemoteTraceId, process.TraceId);
        Assert.Equal(RemoteSpanId, process.ParentSpanId);
        Assert.Equal(process.SpanId, send.ParentSpanId);
        var forwarded = Assert.Single(endpoint.Posts);
        Assert.Equal(send.Id, Field(forwarded, "traceParent"));
        Assert.Equal("vendor=x", Field(forwarded, "traceState"));   // the trace state travels with the trace
    }

    [Fact]
    public async Task DeadLetterCopy_CarriesTheDeadLetterSend()
    {
        using var collector = new Collector();
        var dlq = new MemoryQueueEndPoint("Dlq");
        var broker = Broker(("Dlq", dlq));
        broker.SetDeadLetterEndpoint("Dlq");
        broker.SetMaxMessageAge(TimeSpan.FromMinutes(1));
        var old = new MessageContext<Order> { Message = new Order(), Created = DateTime.UtcNow.AddHours(-1), TraceParent = RemoteParent };

        await broker.ProcessAsync(MessageSerializerJson.SerializeToBytes(old), "In", Ct);

        var send = Assert.Single(collector.Named("nymbroker.send"));
        Assert.Equal("Dlq", send.GetTagItem("messaging.destination.name"));
        var copies = new List<string>();
        await foreach (var item in dlq.ReadAsync(Ct)) copies.Add(item);
        Assert.Equal(send.Id, Field(Encoding.UTF8.GetBytes(Assert.Single(copies)), "traceParent"));
    }

    [Fact]
    public async Task PropagationOff_DropsAReceivedContext_WhenForwarding()
    {
        using var collector = new Collector();
        var endpoint = new RecordingEndPoint();
        var broker = Broker(("Out", endpoint));
        broker.ConfigureTracing(new TracingOptions { PropagateTraceContext = false });
        broker.Route<Order>().To("Out").Build();

        await broker.ProcessAsync(Envelope(RemoteParent), "In", Ct);

        Assert.Null(Field(Assert.Single(endpoint.Posts), "traceParent"));
    }

    [Fact]
    public async Task WithoutAListener_AReceivedContextPassesThrough()
    {
        var endpoint = new RecordingEndPoint();
        var broker = Broker(("Out", endpoint));
        broker.Route<Order>().To("Out").Build();

        await broker.ProcessAsync(Envelope(RemoteParent, "vendor=x"), "In", Ct);

        var forwarded = Assert.Single(endpoint.Posts);
        Assert.Equal(RemoteParent, Field(forwarded, "traceParent"));
        Assert.Equal("vendor=x", Field(forwarded, "traceState"));
    }

    [Fact]
    public void ConfigureTracing_RejectsAnUnknownContinuation()
        => Assert.Throws<ArgumentOutOfRangeException>(() => new ServiceCollection().AddNymBroker()
            .ConfigureTracing(o => o.Continuation = (TraceContinuation)7));
}
