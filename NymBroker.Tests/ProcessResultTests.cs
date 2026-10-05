using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Factory;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;

namespace NymBroker.Tests;

/// <summary>
/// The result <see cref="INymBroker.ProcessAsync(byte[], string?, CancellationToken)"/> returns to the source endpoint,
/// for endpoints that dead-letter natively ("Native") and those that rely on the broker's dead-letter endpoint ("Plain").
/// </summary>
public sealed class ProcessResultTests
{
    private const string Native = "Native";
    private const string Plain = "Plain";
    private const string DeadLetters = "DLQ";

    [MessageName("tests.processresult.order")]
    public sealed class ResultOrder
    {
        public int Id { get; set; }
        public string Payload { get; set; } = "";
    }

    public sealed class OkConsumer : IConsume<ResultOrder>
    {
        public Task ConsumeAsync(ResultOrder message, IMessageContext context, CancellationToken ct = default) => Task.CompletedTask;
    }

    public sealed class ThrowingConsumer : IConsume<ResultOrder>
    {
        public Task ConsumeAsync(ResultOrder message, IMessageContext context, CancellationToken ct = default)
            => throw new InvalidOperationException("consumer failed");
    }

    public sealed class ThrowingSubscriber : ISubscribe<ResultOrder>
    {
        public Task ReceiveAsync(ResultOrder message, IMessageContext context, CancellationToken ct = default)
            => throw new InvalidOperationException("subscriber failed");
    }

    /// <summary>Event-driven test endpoint; records what is posted to it and can be told to fail posts.</summary>
    private sealed class FakeEndPoint(bool usesNativeDeadLetter, bool failPosts = false) : IEndPointEventDriven
    {
        public ConcurrentQueue<byte[]> Posted { get; } = new();
        public bool UsesNativeDeadLetter => usesNativeDeadLetter;

        public Task PostAsync(byte[] message, CancellationToken ct = default)
        {
            if (failPosts) throw new IOException("destination unavailable");
            Posted.Enqueue(message);
            return Task.CompletedTask;
        }

        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
        public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct) => Task.CompletedTask;
        public Task StopListeningAsync() => Task.CompletedTask;
    }

    private sealed record Harness(INymBroker Broker, MemoryQueueEndPoint DeadLetterQueue, FakeEndPoint NativeEndPoint, MessageSerializerJson Serializer)
    {
        public async Task<List<string>> DrainDeadLettersAsync()
        {
            var items = new List<string>();
            await foreach (var item in DeadLetterQueue.ReadAsync(TestContext.Current.CancellationToken)) items.Add(item);
            return items;
        }
    }

    private static Harness Build(Action<NymBrokerBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var nativeEndPoint = new FakeEndPoint(usesNativeDeadLetter: true);

        var builder = services.AddNymBroker()
            .AddMemoryEndPoint(DeadLetters)
            .WithDeadLetterEndpoint(DeadLetters);
        builder.Services.AddKeyedSingleton<IEndPoint>(Native, nativeEndPoint);
        builder.RegisterEndpoint(Native);
        builder.Services.AddKeyedSingleton<IEndPoint>(Plain, new FakeEndPoint(usesNativeDeadLetter: false));
        builder.RegisterEndpoint(Plain);
        configure(builder);
        builder.Build();

        var sp = services.BuildServiceProvider();
        return new Harness(
            sp.GetRequiredService<INymBroker>(),
            (MemoryQueueEndPoint)sp.GetRequiredKeyedService<IEndPoint>(DeadLetters),
            nativeEndPoint,
            sp.GetRequiredService<MessageSerializerJson>());
    }

    private static byte[] Envelope<T>(MessageSerializerJson serializer, T message, DateTime? created = null) where T : class
    {
        var context = new MessageContext<T> { Message = message };
        if (created.HasValue) context.Created = created.Value;
        using var stream = serializer.Serialize(context);
        using var copy = new MemoryStream();
        stream.CopyTo(copy);
        return copy.ToArray();
    }

    // --- Success ---

    [Theory]
    [InlineData(Native)]
    [InlineData(Plain)]
    public async Task Success_ReturnsCompleted(string source)
    {
        var h = Build(b => b.AddConsumer<OkConsumer>());

        var result = await h.Broker.ProcessAsync(Envelope(h.Serializer, new ResultOrder { Id = 1 }), source, TestContext.Current.CancellationToken);

        Assert.Equal(ProcessResult.Completed, result);
        Assert.Empty(await h.DrainDeadLettersAsync());
    }

    // --- Undecodable bytes: dead-lettered on both paths (previously dropped) ---

    [Fact]
    public async Task DeserializationFailure_Native_ReturnsDeadLetter()
    {
        var h = Build(b => b.AddConsumer<OkConsumer>());

        var result = await h.Broker.ProcessAsync("not json at all", Native, TestContext.Current.CancellationToken);

        Assert.Equal(ProcessOutcome.DeadLetter, result.Outcome);
        Assert.Equal(DeadLetterReasons.DeserializationFailed, result.Reason);
        Assert.NotNull(result.Exception);
        Assert.Empty(await h.DrainDeadLettersAsync());
    }

    [Fact]
    public async Task DeserializationFailure_Plain_PostsToDeadLetterEndpoint_AndCompletes()
    {
        var h = Build(b => b.AddConsumer<OkConsumer>());

        var result = await h.Broker.ProcessAsync("not json at all", Plain, TestContext.Current.CancellationToken);

        Assert.Equal(ProcessResult.Completed, result);
        Assert.Equal(["not json at all"], await h.DrainDeadLettersAsync());
    }

    // --- Expired ---

    [Fact]
    public async Task Expired_Native_ReturnsDeadLetter()
    {
        var h = Build(b => b.AddConsumer<OkConsumer>().DiscardMessagesOlderThan(TimeSpan.FromMinutes(1)));
        var raw = Envelope(h.Serializer, new ResultOrder { Id = 2 }, DateTime.UtcNow.AddHours(-1));

        var result = await h.Broker.ProcessAsync(raw, Native, TestContext.Current.CancellationToken);

        Assert.Equal(ProcessOutcome.DeadLetter, result.Outcome);
        Assert.Equal(DeadLetterReasons.Expired, result.Reason);
        Assert.Empty(await h.DrainDeadLettersAsync());
    }

    [Fact]
    public async Task Expired_Plain_PostsToDeadLetterEndpoint_AndCompletes()
    {
        var h = Build(b => b.AddConsumer<OkConsumer>().DiscardMessagesOlderThan(TimeSpan.FromMinutes(1)));
        var raw = Envelope(h.Serializer, new ResultOrder { Id = 2 }, DateTime.UtcNow.AddHours(-1));

        var result = await h.Broker.ProcessAsync(raw, Plain, TestContext.Current.CancellationToken);

        Assert.Equal(ProcessResult.Completed, result);
        Assert.Single(await h.DrainDeadLettersAsync());
    }

    // --- Split message with an unknown compression codec ---

    [Theory]
    [InlineData(Native, ProcessOutcome.DeadLetter, 0)]
    [InlineData(Plain, ProcessOutcome.Completed, 1)]
    public async Task UnknownCompression_IsDeadLettered(string source, ProcessOutcome expected, int expectedOnDeadLetterEndpoint)
    {
        var h = Build(b => b.AddConsumer<OkConsumer>());
        var part = Envelope(h.Serializer, new SplitMessage
        {
            CorrelationId = Guid.NewGuid(),
            CorrelationSequence = 0,
            GroupSize = 1,
            Body = Convert.ToBase64String("payload"u8.ToArray()),
            Compression = "zstd"
        });

        var result = await h.Broker.ProcessAsync(part, source, TestContext.Current.CancellationToken);

        Assert.Equal(expected, result.Outcome);
        if (expected == ProcessOutcome.DeadLetter)
            Assert.Equal(DeadLetterReasons.UnknownCompression, result.Reason);
        Assert.Equal(expectedOnDeadLetterEndpoint, (await h.DrainDeadLettersAsync()).Count);
    }

    // --- Consumer throws: native endpoints retry (transport dead-letters at its limit) ---

    [Fact]
    public async Task ConsumerFailure_Native_ReturnsRetry_WithException()
    {
        var h = Build(b => b.AddConsumer<ThrowingConsumer>());

        var result = await h.Broker.ProcessAsync(Envelope(h.Serializer, new ResultOrder { Id = 3 }), Native, TestContext.Current.CancellationToken);

        Assert.Equal(ProcessOutcome.Retry, result.Outcome);
        Assert.IsType<InvalidOperationException>(result.Exception);
        Assert.Equal("consumer failed", result.Description);
        Assert.Empty(await h.DrainDeadLettersAsync());
    }

    [Fact]
    public async Task ConsumerFailure_Plain_PostsToDeadLetterEndpoint_AndCompletes()
    {
        var h = Build(b => b.AddConsumer<ThrowingConsumer>());

        var result = await h.Broker.ProcessAsync(Envelope(h.Serializer, new ResultOrder { Id = 3 }), Plain, TestContext.Current.CancellationToken);

        Assert.Equal(ProcessResult.Completed, result);
        Assert.Single(await h.DrainDeadLettersAsync());
    }

    // --- Topic fan-out throws ---

    [Theory]
    [InlineData(Native, ProcessOutcome.Retry, 0)]
    [InlineData(Plain, ProcessOutcome.Completed, 1)]
    public async Task TopicFailure_IsRetriedNatively_OrDeadLettered(string source, ProcessOutcome expected, int expectedOnDeadLetterEndpoint)
    {
        var h = Build(b => b.AddTopic<ResultOrder>("orders").SubscribeWith<ThrowingSubscriber>().Build());

        var result = await h.Broker.ProcessAsync(Envelope(h.Serializer, new ResultOrder { Id = 4 }), source, TestContext.Current.CancellationToken);

        Assert.Equal(expected, result.Outcome);
        Assert.Equal(expectedOnDeadLetterEndpoint, (await h.DrainDeadLettersAsync()).Count);
    }

    // --- Posting to a route's destination throws: Retry for every source ---

    [Theory]
    [InlineData(Native)]
    [InlineData(Plain)]
    public async Task RouteDestinationFailure_ReturnsRetry(string source)
    {
        var h = Build(b =>
        {
            b.Services.AddKeyedSingleton<IEndPoint>("Failing", new FakeEndPoint(usesNativeDeadLetter: false, failPosts: true));
            b.RegisterEndpoint("Failing");
        });
        h.Broker.Route<ResultOrder>().To("Failing").Build();

        var result = await h.Broker.ProcessAsync(Envelope(h.Serializer, new ResultOrder { Id = 5 }), source, TestContext.Current.CancellationToken);

        Assert.Equal(ProcessOutcome.Retry, result.Outcome);
        Assert.IsType<IOException>(result.Exception);
        Assert.Empty(await h.DrainDeadLettersAsync());
    }

    // --- Split messages: the part that completes the group carries the reassembled result ---

    [Fact]
    public async Task SplitMessage_LastPartCarriesInnerResult()
    {
        var h = Build(b => b.AddConsumer<ThrowingConsumer>());
        var payload = string.Concat(Enumerable.Range(0, 60).Select(_ => Guid.NewGuid().ToString("N")));
        await h.Broker.PostAsync(Native, new ResultOrder { Id = 6, Payload = payload }, TestContext.Current.CancellationToken,
            splitThresholdBytes: 400, compress: false);
        var parts = h.NativeEndPoint.Posted.ToList();
        Assert.True(parts.Count > 1);

        var results = new List<ProcessResult>();
        foreach (var part in parts)
            results.Add(await h.Broker.ProcessAsync(part, Native, TestContext.Current.CancellationToken));

        Assert.All(results.Take(parts.Count - 1), r => Assert.Equal(ProcessResult.Completed, r));
        Assert.Equal(ProcessOutcome.Retry, results[^1].Outcome);
    }

    // --- PublishAsync has no transport: non-native behaviour, a Retry surfaces as an exception ---

    [Fact]
    public async Task PublishAsync_ConsumerFailure_IsDeadLetteredByBroker_WithoutThrowing()
    {
        var h = Build(b => b.AddConsumer<ThrowingConsumer>());

        await h.Broker.PublishAsync(new ResultOrder { Id = 7 }, TestContext.Current.CancellationToken);

        Assert.Single(await h.DrainDeadLettersAsync());
    }

    [Fact]
    public async Task PublishAsync_RouteFailure_Throws()
    {
        var h = Build(b =>
        {
            b.Services.AddKeyedSingleton<IEndPoint>("Failing", new FakeEndPoint(usesNativeDeadLetter: false, failPosts: true));
            b.RegisterEndpoint("Failing");
        });
        h.Broker.Route<ResultOrder>().To("Failing").Build();

        await Assert.ThrowsAsync<IOException>(() => h.Broker.PublishAsync(new ResultOrder { Id = 8 }, TestContext.Current.CancellationToken));
    }

    // --- Defaults and helpers ---

    [Fact]
    public void InProcessEndpoints_DoNotDeadLetterNatively()
    {
        Assert.False(((IEndPointEventDriven)new MemoryQueueEndPoint("m")).UsesNativeDeadLetter);
    }

    [Fact]
    public void FailureText_CombinesReasonAndDescription()
    {
        Assert.Equal("Expired: too old", ProcessResult.DeadLetter(DeadLetterReasons.Expired, "too old").FailureText);
        Assert.Equal("boom", ProcessResult.Retry(new InvalidOperationException("boom")).FailureText);
        Assert.Null(ProcessResult.Completed.FailureText);
    }

    [Fact]
    public async Task MemoryEndpoint_LogsAndContinues_WhenResultIsNotCompleted()
    {
        var logger = new CapturingLogger<MemoryQueueEndPoint>();
        var ep = new MemoryQueueEndPoint("mem", logger: logger);
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await ep.StartListeningAsync((_, _) =>
        {
            if (Interlocked.Increment(ref calls) == 2) handled.TrySetResult();
            return Task.FromResult(calls == 1 ? ProcessResult.Retry(new InvalidOperationException("nope")) : ProcessResult.Completed);
        }, cts.Token);

        await ep.EnqueueAsync("{}", TestContext.Current.CancellationToken);
        await ep.EnqueueAsync("{}", TestContext.Current.CancellationToken);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Contains(logger.Entries, e => e.Level == Microsoft.Extensions.Logging.LogLevel.Error && e.Message.Contains("cannot be redelivered"));
        await cts.CancelAsync();
    }
}
