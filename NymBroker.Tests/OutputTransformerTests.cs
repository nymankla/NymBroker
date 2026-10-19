using System.Collections.Immutable;
using System.Text;
using System.Xml.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core.Aggregator;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.PubSub;
using NymBroker.Core.Serialize;
using NymBroker.Core.Transform;

namespace NymBroker.Tests;

/// <summary>Output transformers: every send to an endpoint is converted to the destination's format (#83).</summary>
public sealed class OutputTransformerTests
{
    private sealed class Order
    {
        public string Id { get; set; } = "";
        public decimal Amount { get; set; }
    }

    /// <summary>JSON → XML; drops orders with id "skip".</summary>
    private sealed class OrderToXml : IOutputTransformer
    {
        public byte[]? Transform(RawMessageContext message, string destinationEndpoint)
        {
            var id = message.RawMessage.GetProperty("id").GetString();
            if (id == "skip") return null;
            var xml = new XElement("Order",
                new XElement("Id", id),
                new XElement("Amount", message.RawMessage.GetProperty("amount").GetDecimal()),
                new XElement("CorrelationId", message.CorrelationId));
            return Encoding.UTF8.GetBytes(xml.ToString(SaveOptions.DisableFormatting));
        }
    }

    private sealed class ThrowingTransformer : IOutputTransformer
    {
        public byte[]? Transform(RawMessageContext message, string destinationEndpoint)
            => throw new InvalidOperationException("transform failed");
    }

    private static (NymBrokerImpl Broker, MemoryQueueEndPoint Legacy) BuildBroker(IOutputTransformer? transformer = null)
    {
        var sp = new ServiceCollection().AddLogging().BuildServiceProvider();
        var broker = new NymBrokerImpl(
            new MessageSerializerJson(),
            new AggregatorImpl(NullLogger<AggregatorImpl>.Instance),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance);
        var legacy = new MemoryQueueEndPoint("Legacy", mode: EndpointMode.WriteOnly);
        broker.AddEndpoint("Legacy", legacy);
        broker.AddEndpoint("In", new MemoryQueueEndPoint("In"));
        broker.AddOutputTransformer(transformer ?? new OrderToXml(), "Legacy");
        return (broker, legacy);
    }

    private static async Task<List<string>> DrainAsync(MemoryQueueEndPoint endpoint)
    {
        var items = new List<string>();
        await foreach (var item in endpoint.ReadAsync(TestContext.Current.CancellationToken)) items.Add(item);
        return items;
    }

    private static string Envelope(string id) =>
        $$$"""{"id":"{{{Guid.NewGuid()}}}","messageType":"{{{MessageTypeName.Get(typeof(Order))}}}","message":{"id":"{{{id}}}","amount":12.5}}""";

    [Fact]
    public async Task PostAsync_SendsTheTransformedBytes()
    {
        var (broker, legacy) = BuildBroker();

        await broker.PostAsync("Legacy", new Order { Id = "A-1", Amount = 12.5m }, TestContext.Current.CancellationToken);

        var xml = XElement.Parse(Assert.Single(await DrainAsync(legacy)));
        Assert.Equal("A-1", xml.Element("Id")!.Value);
        Assert.Equal("12.5", xml.Element("Amount")!.Value);
    }

    [Fact]
    public async Task PostAsync_TransformerReturnsNull_SendsNothing()
    {
        var (broker, legacy) = BuildBroker();

        await broker.PostAsync("Legacy", new Order { Id = "skip" }, TestContext.Current.CancellationToken);

        Assert.Empty(await DrainAsync(legacy));
    }

    [Fact]
    public async Task PostBatchAsync_TransformsEachMessage_AndLeavesOutDropped()
    {
        var (broker, legacy) = BuildBroker();

        await broker.PostBatchAsync("Legacy",
            [new Order { Id = "A-1" }, new Order { Id = "skip" }, new Order { Id = "A-2" }],
            TestContext.Current.CancellationToken);

        var ids = (await DrainAsync(legacy)).Select(x => XElement.Parse(x).Element("Id")!.Value);
        Assert.Equal(["A-1", "A-2"], ids);
    }

    [Fact]
    public async Task Route_ToTransformedEndpoint_SendsTheTransformedBytes()
    {
        var (broker, legacy) = BuildBroker();
        broker.Route<Order>().To("Legacy").WhenFrom("In").Build();

        var result = await broker.ProcessAsync(Envelope("R-1"), "In", TestContext.Current.CancellationToken);

        Assert.Equal(ProcessOutcome.Completed, result.Outcome);
        Assert.Equal("R-1", XElement.Parse(Assert.Single(await DrainAsync(legacy))).Element("Id")!.Value);
    }

    [Fact]
    public async Task Route_TransformerThrows_ReturnsRetry()
    {
        var (broker, _) = BuildBroker(new ThrowingTransformer());
        broker.Route<Order>().To("Legacy").WhenFrom("In").Build();

        var result = await broker.ProcessAsync(Envelope("R-1"), "In", TestContext.Current.CancellationToken);

        Assert.Equal(ProcessOutcome.Retry, result.Outcome);
    }

    [Fact]
    public async Task TopicEndpoint_ReceivesTheTransformedBytes()
    {
        var (broker, legacy) = BuildBroker();
        broker.AddTopic(new TopicContext
        {
            TopicName = "orders",
            MessageType = typeof(Order),
            SubscriberEndpoints = ImmutableList.Create("Legacy")
        });

        await broker.PublishAsync("orders", new Order { Id = "T-1" }, TestContext.Current.CancellationToken);

        Assert.Equal("T-1", XElement.Parse(Assert.Single(await DrainAsync(legacy))).Element("Id")!.Value);
    }

    [Fact]
    public async Task OtherEndpoints_StillReceiveTheEnvelope()
    {
        var (broker, _) = BuildBroker();
        var plain = new MemoryQueueEndPoint("Plain");
        broker.AddEndpoint("Plain", plain);

        await broker.PostAsync("Plain", new Order { Id = "P-1" }, TestContext.Current.CancellationToken);

        Assert.Contains("\"messageType\"", Assert.Single(await DrainAsync(plain)));
    }

    [Fact]
    public async Task PostAsync_WithSplitThreshold_Throws()
    {
        var (broker, legacy) = BuildBroker();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            broker.PostAsync("Legacy", new Order { Id = "A-1" }, TestContext.Current.CancellationToken, splitThresholdBytes: 1024));

        Assert.Contains("cannot be split", ex.Message);
        Assert.Empty(await DrainAsync(legacy));
    }

    [Fact]
    public async Task PostRawAsync_NotAnEnvelope_Throws()
    {
        var (broker, _) = BuildBroker();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            broker.PostRawAsync("Legacy", "not json"u8.ToArray(), TestContext.Current.CancellationToken));

        Assert.Contains("needs a NymBroker envelope", ex.Message);
    }

    [Fact]
    public async Task StartAsync_BrokerListensOnTransformedEndpoint_Throws()
    {
        var (broker, _) = BuildBroker();
        broker.AddOutputTransformer(new OrderToXml(), "In");   // In is ReadWrite: the broker would read XML back

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => broker.StartAsync(TestContext.Current.CancellationToken));

        Assert.Contains("'In' has an output transformer and the broker also listens on it", ex.Message);
    }

    [Fact]
    public async Task StartAsync_TransformedEndpointNotRegistered_Throws()
    {
        var (broker, _) = BuildBroker();
        broker.AddOutputTransformer(new OrderToXml(), "Missing");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => broker.StartAsync(TestContext.Current.CancellationToken));

        Assert.Contains("'Missing' is not a registered endpoint", ex.Message);
    }

    [Fact]
    public async Task Builder_AddOutputTransformer_RegistersAndApplies()
    {
        var services = new ServiceCollection().AddLogging();
        services.AddNymBroker()
            .AddMemoryEndPoint("Legacy", mode: EndpointMode.WriteOnly)
            .AddOutputTransformer<OrderToXml>("Legacy")
            .Build();
        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<INymBroker>();
        await broker.StartAsync(TestContext.Current.CancellationToken);

        await broker.PostAsync("Legacy", new Order { Id = "B-1" }, TestContext.Current.CancellationToken);

        var legacy = (MemoryQueueEndPoint)sp.GetRequiredKeyedService<IEndPoint>("Legacy");
        Assert.Equal("B-1", XElement.Parse(Assert.Single(await DrainAsync(legacy))).Element("Id")!.Value);
        await broker.StopAsync(TestContext.Current.CancellationToken);
    }
}
