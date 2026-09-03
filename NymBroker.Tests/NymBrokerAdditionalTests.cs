using System.Text;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Consume;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Filter;
using NymBroker.Core.Impl;
using NymBroker.Core.PubSub;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;
using NymBroker.Core.Splitter;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace NymBroker.Tests;

public sealed class NymBrokerAdditionalTests
{
    private sealed class StockMessage { public string Symbol { get; set; } = ""; }

    private sealed class StockConsumer : IConsume<StockMessage>
    {
        public List<StockMessage> Received { get; } = [];

        public Task ConsumeAsync(StockMessage message, IMessageContext context, CancellationToken ct = default)
        {
            Received.Add(message);
            return Task.CompletedTask;
        }
    }

    private static (NymBrokerImpl Broker, MemoryQueueEndPoint Dest) BuildBroker()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<MessageSerializerJson>();
        services.AddSingleton<IAggregator, AggregatorImpl>();
        var sp = services.BuildServiceProvider();

        var dest = new MemoryQueueEndPoint("Dest");
        var broker = new NymBrokerImpl(
            sp.GetRequiredService<MessageSerializerJson>(),
            sp.GetRequiredService<IAggregator>(),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance);

        broker.AddEndpoint("Dest", dest);
        return (broker, dest);
    }

    private static async Task<string> SerializeAsync<T>(T message) where T : class
    {
        var serializer = new MessageSerializerJson();
        var ctx = new MessageContext<T> { Message = message };
        using var stream = serializer.Serialize(ctx);
        using var reader = new StreamReader(stream);
        return await reader.ReadToEndAsync();
    }

    // --- PostAsync ---

    [Fact]
    public async Task PostAsync_Typed_SendsSerializedMessageToEndpoint()
    {
        var (broker, dest) = BuildBroker();
        await broker.PostAsync("Dest", new StockMessage { Symbol = "AAPL" }, TestContext.Current.CancellationToken);

        var items = new List<string>();
        await foreach (var item in dest.ReadAsync(TestContext.Current.CancellationToken)) items.Add(item);

        Assert.Single(items);
        Assert.Contains("AAPL", items[0]);
    }

    [Fact]
    public async Task PostAsync_Typed_ThrowsInvalidOperationException_WhenEndpointNotFound()
    {
        var (broker, _) = BuildBroker();
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => broker.PostAsync("Unknown", new StockMessage(), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task PostAsync_Stream_ThrowsInvalidOperationException_WhenEndpointNotFound()
    {
        var (broker, _) = BuildBroker();
        using var ms = new MemoryStream(System.Text.Encoding.UTF8.GetBytes("{}"));
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => broker.PostAsync("Unknown", ms, TestContext.Current.CancellationToken));
    }

    // --- ProcessAsync with invalid JSON ---

    [Fact]
    public async Task ProcessAsync_DoesNotThrow_WhenJsonIsInvalid()
    {
        var (broker, dest) = BuildBroker();
        // Should log the error and return gracefully rather than throw.
        await broker.ProcessAsync("not-valid-json", "Source", TestContext.Current.CancellationToken);

        var items = new List<string>();
        await foreach (var item in dest.ReadAsync(TestContext.Current.CancellationToken)) items.Add(item);
        Assert.Empty(items);
    }

    // --- Filters ---

    private sealed class DiscardFilter : IMessageFilter
    {
        public IMessageContext? Filter(IMessageContext context) => null;
    }

    private sealed class PassThroughFilter : IMessageFilter
    {
        public int Calls { get; private set; }
        public IMessageContext? Filter(IMessageContext context) { Calls++; return context; }
    }

    [Fact]
    public async Task ProcessAsync_DiscardsMessage_WhenFilterReturnsNull()
    {
        var (broker, dest) = BuildBroker();
        broker.AddFilter(new DiscardFilter());
        broker.Route<StockMessage>().To("Dest").Build();

        await broker.ProcessAsync(await SerializeAsync(new StockMessage { Symbol = "GOOG" }), null, TestContext.Current.CancellationToken);

        var items = new List<string>();
        await foreach (var item in dest.ReadAsync(TestContext.Current.CancellationToken)) items.Add(item);
        Assert.Empty(items);
    }

    [Fact]
    public async Task AddFilter_PassThroughFilter_IsCalledForEachMessage()
    {
        var (broker, _) = BuildBroker();
        var filter = new PassThroughFilter();
        broker.AddFilter(filter);

        await broker.ProcessAsync(await SerializeAsync(new StockMessage { Symbol = "X" }), null, TestContext.Current.CancellationToken);

        Assert.Equal(1, filter.Calls);
    }

    [Fact]
    public async Task AddFilter_ReturnsThisBroker_ForFluentChaining()
    {
        var (broker, _) = BuildBroker();
        var result = broker.AddFilter(new PassThroughFilter());
        Assert.Same(broker, result);
    }

    // --- Route to unknown endpoint ---

    [Fact]
    public async Task ProcessAsync_HandlesRouteToUnknownEndpoint_Gracefully()
    {
        var (broker, _) = BuildBroker();
        broker.Route<StockMessage>().To("NonExistent").Build();

        // Should log a warning but not throw
        await broker.ProcessAsync(await SerializeAsync(new StockMessage { Symbol = "IBM" }), null, TestContext.Current.CancellationToken);
    }

    // --- Message with no consumer and no matching route ---

    [Fact]
    public async Task ProcessAsync_DoesNotThrow_WhenMessageTypeHasNoConsumerOrRoute()
    {
        var (broker, _) = BuildBroker();
        // No route, no consumer registered — should complete without throwing.
        await broker.ProcessAsync(await SerializeAsync(new StockMessage { Symbol = "AMZN" }), null, TestContext.Current.CancellationToken);
    }

    // --- AddScheduledAction overloads ---

    private sealed class InvocationState { public bool Invoked { get; set; } }

    [Fact]
    public async Task AddScheduledAction_WithOneParam_ExecutesAction()
    {
        var (broker, _) = BuildBroker();
        var state = new InvocationState();
        using var ready = new ManualResetEventSlim(false);

        broker.AddScheduledAction(TimeSpan.FromMilliseconds(50), s =>
        {
            s.Invoked = true;
            ready.Set();
        }, state);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await broker.StartAsync(cts.Token);

        Assert.True(ready.Wait(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken));
        await broker.StopAsync(TestContext.Current.CancellationToken);
        Assert.True(state.Invoked);
    }

    [Fact]
    public async Task AddScheduledAction_WithTwoParams_ExecutesAction()
    {
        var (broker, _) = BuildBroker();
        var sum = 0;
        using var ready = new ManualResetEventSlim(false);

        broker.AddScheduledAction(TimeSpan.FromMilliseconds(50), (a, b) =>
        {
            sum = a + b;
            ready.Set();
        }, 3, 4);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await broker.StartAsync(cts.Token);

        Assert.True(ready.Wait(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken));
        await broker.StopAsync(TestContext.Current.CancellationToken);
        Assert.Equal(7, sum);
    }

    // --- AddScheduledAction null-guard ---

    [Fact]
    public void AddScheduledAction_ThrowsArgumentNullException_WhenActionIsNull()
    {
        var (broker, _) = BuildBroker();
        Assert.Throws<ArgumentNullException>(() => broker.AddScheduledAction(TimeSpan.FromSeconds(1), (Action)null!));
    }

    [Fact]
    public void AddScheduledAction_WithParam_ThrowsArgumentNullException_WhenActionIsNull()
    {
        var (broker, _) = BuildBroker();
        Assert.Throws<ArgumentNullException>(() => broker.AddScheduledAction(TimeSpan.FromSeconds(1), (Action<int>)null!, 1));
    }

    [Fact]
    public void AddScheduledAction_WithTwoParams_ThrowsArgumentNullException_WhenActionIsNull()
    {
        var (broker, _) = BuildBroker();
        Assert.Throws<ArgumentNullException>(() => broker.AddScheduledAction(TimeSpan.FromSeconds(1), (Action<int, int>)null!, 1, 2));
    }

    [Fact]
    public void AddScheduledAction_CronExpression_ThrowsArgumentException_WhenExpressionIsEmpty()
    {
        var (broker, _) = BuildBroker();
        Assert.Throws<ArgumentException>(() => broker.AddScheduledAction("", (InvocationState s) => { }, new InvocationState()));
    }

    // --- Split+Aggregate end-to-end ---

    [Fact]
    public async Task ProcessAsync_ReassemblesSplitMessages_ThenDispatchesToConsumer()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var consumer = new StockConsumer();
        services.AddKeyedSingleton<IMessageConsumer>(nameof(StockConsumer), consumer);
        services.AddSingleton<MessageSerializerJson>();
        services.AddSingleton<IAggregator, AggregatorImpl>();
        var sp = services.BuildServiceProvider();

        var broker = new NymBrokerImpl(
            sp.GetRequiredService<MessageSerializerJson>(),
            sp.GetRequiredService<IAggregator>(),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance);

        broker.RegisterConsumer(typeof(StockMessage), nameof(StockConsumer));

        var serializer = new MessageSerializerJson();
        var original = new MessageContext<StockMessage> { Message = new StockMessage { Symbol = "TEST" } };
        using var originalStream = serializer.Serialize(original);
        var originalBytes = ((MemoryStream)originalStream).ToArray();

        var splitter = new Core.Splitter.SplitterImpl();
        var parts = splitter.Split(originalBytes, new Core.Splitter.DefaultSplitCondition(10));

        Assert.True(parts.Count > 1, "Payload must be split into multiple parts for this test to be meaningful.");

        foreach (var part in parts)
        {
            var partCtx = new MessageContext<SplitMessage> { Message = part };
            using var partStream = serializer.Serialize(partCtx);
            using var partReader = new StreamReader(partStream);
            await broker.ProcessAsync(await partReader.ReadToEndAsync(TestContext.Current.CancellationToken), null, TestContext.Current.CancellationToken);
        }

        Assert.Single(consumer.Received);
        Assert.Equal("TEST", consumer.Received[0].Symbol);
    }

    // --- PostAsync auto-split ---

    private sealed class BigMessage { public string Payload { get; set; } = ""; }

    private sealed class BigMessageConsumer : IConsume<BigMessage>
    {
        public List<BigMessage> Received { get; } = [];

        public Task ConsumeAsync(BigMessage message, IMessageContext context, CancellationToken ct = default)
        {
            Received.Add(message);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task PostAsync_Typed_SplitsIntoMultipleParts_WhenSplitThresholdExceeded()
    {
        var (broker, dest) = BuildBroker();
        var big = new BigMessage { Payload = new string('x', 500) };

        // compress: false — this test exercises the raw splitting mechanic, independent of compression.
        await broker.PostAsync("Dest", big, TestContext.Current.CancellationToken, splitThresholdBytes: 64, compress: false);

        var items = new List<string>();
        await foreach (var item in dest.ReadAsync(TestContext.Current.CancellationToken)) items.Add(item);

        Assert.True(items.Count > 1, "Expected the oversized message to be split into multiple parts.");
        Assert.All(items, item => Assert.Contains("correlationId", item));
        Assert.DoesNotContain(items, item => item.Contains("BigMessage") || item.Contains(nameof(NymBrokerAdditionalTests)));
    }

    // --- PostAsync auto-split + compression ---

    [Fact]
    public async Task PostAsync_Typed_CompressibleSplitParts_AreSmallerThanUncompressed()
    {
        var repeatedText = string.Concat(Enumerable.Repeat("The quick brown fox jumps over the lazy dog. ", 200));

        var (broker1, dest1) = BuildBroker();
        await broker1.PostAsync("Dest", new BigMessage { Payload = repeatedText }, TestContext.Current.CancellationToken, splitThresholdBytes: 1000, compress: true);
        var compressedItems = new List<string>();
        await foreach (var item in dest1.ReadAsync(TestContext.Current.CancellationToken)) compressedItems.Add(item);

        var (broker2, dest2) = BuildBroker();
        await broker2.PostAsync("Dest", new BigMessage { Payload = repeatedText }, TestContext.Current.CancellationToken, splitThresholdBytes: 1000, compress: false);
        var uncompressedItems = new List<string>();
        await foreach (var item in dest2.ReadAsync(TestContext.Current.CancellationToken)) uncompressedItems.Add(item);

        // Note: highly repetitive text like this can compress well enough that even the
        // compressed form collapses to a single part — the meaningful assertion is total wire
        // size, not part count.
        Assert.Contains(compressedItems, item => item.Contains("\"compression\":\"brotli\""));

        var compressedTotalBytes = compressedItems.Sum(i => Encoding.UTF8.GetByteCount(i));
        var uncompressedTotalBytes = uncompressedItems.Sum(i => Encoding.UTF8.GetByteCount(i));
        Assert.True(compressedTotalBytes < uncompressedTotalBytes,
            $"Expected compression to reduce total wire size: compressed={compressedTotalBytes}, uncompressed={uncompressedTotalBytes}");
    }

    // Compressor stub whose output is always at least as large as the input — exercises the
    // "compression didn't help" fallback deterministically (real codecs can shrink even
    // superficially "random" text, like Base64, since it isn't full-entropy per byte).
    private sealed class NoBenefitCompressor : ICompressor
    {
        public string Name => "no-benefit";
        public byte[] Compress(byte[] data) => [.. data, 0];
        public byte[] Decompress(byte[] data) => data[..^1];
    }

    [Fact]
    public async Task PostAsync_Typed_SkipsCompression_WhenItDoesNotReduceSize()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<MessageSerializerJson>();
        services.AddSingleton<IAggregator, AggregatorImpl>();
        var sp = services.BuildServiceProvider();

        var broker = new NymBrokerImpl(
            sp.GetRequiredService<MessageSerializerJson>(),
            sp.GetRequiredService<IAggregator>(),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance,
            splitter: null,
            compressor: new NoBenefitCompressor());

        var dest = new MemoryQueueEndPoint("Dest");
        broker.AddEndpoint("Dest", dest);

        await broker.PostAsync("Dest", new BigMessage { Payload = new string('x', 500) }, TestContext.Current.CancellationToken, splitThresholdBytes: 64, compress: true);

        var items = new List<string>();
        await foreach (var item in dest.ReadAsync(TestContext.Current.CancellationToken)) items.Add(item);

        Assert.True(items.Count > 1, "Expected the oversized message to still be split into multiple parts.");
        Assert.All(items, item => Assert.DoesNotContain("no-benefit", item));
    }

    [Fact]
    public async Task PostAsync_Typed_CompressionAloneUnderThreshold_ProducesSinglePartGroup_AndReassembles()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var consumer = new BigMessageConsumer();
        services.AddKeyedSingleton<IMessageConsumer>(nameof(BigMessageConsumer), consumer);
        services.AddSingleton<MessageSerializerJson>();
        services.AddSingleton<IAggregator, AggregatorImpl>();
        var sp = services.BuildServiceProvider();

        var broker = new NymBrokerImpl(
            sp.GetRequiredService<MessageSerializerJson>(),
            sp.GetRequiredService<IAggregator>(),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance);

        broker.RegisterConsumer(typeof(BigMessage), nameof(BigMessageConsumer));
        var dest = new MemoryQueueEndPoint("Dest");
        broker.AddEndpoint("Dest", dest);

        // Highly compressible payload: raw envelope exceeds the threshold, but compresses to well under it —
        // must still collapse to a single-part SplitMessage group (compressed bytes aren't valid JSON on their own).
        var payload = new string('x', 5000);
        await broker.PostAsync("Dest", new BigMessage { Payload = payload }, TestContext.Current.CancellationToken, splitThresholdBytes: 2000, compress: true);

        var items = new List<string>();
        await foreach (var item in dest.ReadAsync(TestContext.Current.CancellationToken)) items.Add(item);

        Assert.Single(items);
        Assert.Contains("\"groupSize\":1", items[0]);
        Assert.Contains("\"compression\":\"brotli\"", items[0]);

        await broker.ProcessAsync(items[0], null, TestContext.Current.CancellationToken);

        Assert.Single(consumer.Received);
    }

    [Fact]
    public async Task PostAsync_Typed_DoesNotSplit_WhenUnderThreshold()
    {
        var (broker, dest) = BuildBroker();
        await broker.PostAsync("Dest", new StockMessage { Symbol = "AAPL" }, TestContext.Current.CancellationToken, splitThresholdBytes: 1024 * 1024);

        var items = new List<string>();
        await foreach (var item in dest.ReadAsync(TestContext.Current.CancellationToken)) items.Add(item);

        Assert.Single(items);
        Assert.Contains("AAPL", items[0]);
    }

    [Fact]
    public async Task PostAsync_Typed_SplitParts_ReassembleToOriginalMessage()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var consumer = new StockConsumer();
        services.AddKeyedSingleton<IMessageConsumer>(nameof(StockConsumer), consumer);
        services.AddSingleton<MessageSerializerJson>();
        services.AddSingleton<IAggregator, AggregatorImpl>();
        var sp = services.BuildServiceProvider();

        var broker = new NymBrokerImpl(
            sp.GetRequiredService<MessageSerializerJson>(),
            sp.GetRequiredService<IAggregator>(),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance);

        broker.RegisterConsumer(typeof(StockMessage), nameof(StockConsumer));
        var dest = new MemoryQueueEndPoint("Dest");
        broker.AddEndpoint("Dest", dest);

        await broker.PostAsync("Dest", new StockMessage { Symbol = "TEST" }, TestContext.Current.CancellationToken, splitThresholdBytes: 10);

        var parts = new List<string>();
        await foreach (var item in dest.ReadAsync(TestContext.Current.CancellationToken)) parts.Add(item);
        Assert.True(parts.Count > 1, "Payload must be split into multiple parts for this test to be meaningful.");

        foreach (var part in parts)
            await broker.ProcessAsync(part, null, TestContext.Current.CancellationToken);

        Assert.Single(consumer.Received);
        Assert.Equal("TEST", consumer.Received[0].Symbol);
    }
}
