using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.PubSub;
using NymBroker.Core.Serialize;

namespace NymBroker.Tests;

/// <summary>PostRawAsync, and the guard that keeps bytes and streams out of the serializing methods (#74).</summary>
public sealed class PostRawTests
{
    private static (NymBrokerImpl Broker, MemoryQueueEndPoint Dest) BuildBroker()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var sp = services.BuildServiceProvider();

        var dest = new MemoryQueueEndPoint("Dest");
        var broker = new NymBrokerImpl(
            new MessageSerializerJson(),
            new AggregatorImpl(NullLogger<AggregatorImpl>.Instance),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance);
        broker.AddEndpoint("Dest", dest);
        return (broker, dest);
    }

    private static async Task<List<string>> DrainAsync(MemoryQueueEndPoint endpoint)
    {
        var items = new List<string>();
        await foreach (var item in endpoint.ReadAsync(TestContext.Current.CancellationToken)) items.Add(item);
        return items;
    }

    // --- PostRawAsync ---

    [Fact]
    public async Task PostRawAsync_Bytes_PostsThemUnchanged()
    {
        var (broker, dest) = BuildBroker();

        await broker.PostRawAsync("Dest", "ORD-1,Alice,49.90"u8.ToArray(), TestContext.Current.CancellationToken);

        Assert.Equal("ORD-1,Alice,49.90", Assert.Single(await DrainAsync(dest)));
    }

    [Fact]
    public async Task PostRawAsync_CopiesTheBytes_SoTheCallerCanReuseItsBuffer()
    {
        var (broker, dest) = BuildBroker();
        var buffer = "first"u8.ToArray();

        await broker.PostRawAsync("Dest", buffer, TestContext.Current.CancellationToken);
        "XXXXX"u8.CopyTo(buffer);

        Assert.Equal("first", Assert.Single(await DrainAsync(dest)));
    }

    [Fact]
    public async Task PostRawAsync_Stream_ReadsFromTheCurrentPosition()
    {
        var (broker, dest) = BuildBroker();
        using var stream = new MemoryStream("skip|payload"u8.ToArray()) { Position = 5 };

        await broker.PostRawAsync("Dest", stream, TestContext.Current.CancellationToken);

        Assert.Equal("payload", Assert.Single(await DrainAsync(dest)));
    }

    [Fact]
    public async Task PostRawAsync_NonSeekableStream_Works()
    {
        var (broker, dest) = BuildBroker();
        using var stream = new NonSeekableStream("from a request body"u8.ToArray());

        await broker.PostRawAsync("Dest", stream, TestContext.Current.CancellationToken);

        Assert.Equal("from a request body", Assert.Single(await DrainAsync(dest)));
    }

    [Fact]
    public async Task PostRawAsync_SplitThreshold_SplitsLargePayloads()
    {
        var (broker, dest) = BuildBroker();
        var payload = Encoding.UTF8.GetBytes(new string('x', 10_000));

        await broker.PostRawAsync("Dest", payload, TestContext.Current.CancellationToken, splitThresholdBytes: 2_000, compress: false);

        var parts = await DrainAsync(dest);
        Assert.True(parts.Count > 1);
        Assert.All(parts, p => Assert.Contains(MessageTypeName.Get(typeof(SplitMessage)), p));
    }

    // --- The guard ---

    public static TheoryData<string> RawValues => ["stream", "bytes", "memory"];

    private static object Raw(string kind) => kind switch
    {
        "stream" => new MemoryStream("{}"u8.ToArray()),
        "bytes" => "{}"u8.ToArray(),
        _ => new ReadOnlyMemory<byte>("{}"u8.ToArray())   // boxed: only reachable through T = object
    };

    [Theory]
    [MemberData(nameof(RawValues))]
    public async Task SerializingMethods_RejectBytesAndStreams_AndPostNothing(string kind)
    {
        var (broker, dest) = BuildBroker();
        var ct = TestContext.Current.CancellationToken;
        var raw = Raw(kind);

        await AssertPointsToPostRawAsync(() => broker.PostAsync("Dest", raw, ct));
        await AssertPointsToPostRawAsync(() => broker.PostBatchAsync("Dest", [raw], ct));
        await AssertPointsToPostRawAsync(() => broker.PublishAsync(raw, ct));
        await AssertPointsToPostRawAsync(() => broker.PublishAsync("no.such.topic", raw, ct));
        await AssertPointsToPostRawAsync(() => broker.PublishBatchAsync([raw], ct));
        await AssertPointsToPostRawAsync(() => broker.PublishBatchAsync("no.such.topic", [raw], ct));

        Assert.Empty(await DrainAsync(dest));
    }

    [Fact]
    public async Task PostAsync_ConcreteStreamType_IsRejected_InsteadOfSerialized()
    {
        // The original trap: without a cast this binds to PostAsync<MemoryStream>.
        var (broker, _) = BuildBroker();
        using var stream = new MemoryStream("{}"u8.ToArray());

        await AssertPointsToPostRawAsync(() => broker.PostAsync("Dest", stream, TestContext.Current.CancellationToken));
    }

    private static async Task AssertPointsToPostRawAsync(Func<Task> call)
    {
        var ex = await Assert.ThrowsAsync<ArgumentException>(call);
        Assert.Contains("PostRawAsync", ex.Message);
    }

    private sealed class NonSeekableStream(byte[] data) : MemoryStream(data)
    {
        public override bool CanSeek => false;
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override long Seek(long offset, SeekOrigin loc) => throw new NotSupportedException();
        public override long Length => throw new NotSupportedException();
    }
}
