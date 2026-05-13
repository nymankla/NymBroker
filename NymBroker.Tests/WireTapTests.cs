using NymBroker.Core.Aggregator;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.PubSub;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace NymBroker.Tests;

public sealed class WireTapTests
{
    private sealed class Event { public string Name { get; set; } = ""; }

    private sealed class EventConsumer : IConsume<Event>
    {
        public List<Event> Received { get; } = [];
        public Task ConsumeAsync(Event message, IMessageContext context, CancellationToken ct = default)
        {
            Received.Add(message);
            return Task.CompletedTask;
        }
    }

    private static (NymBrokerImpl broker, EventConsumer consumer, MemoryQueueEndPoint tap,
        MessageSerializerJson serializer)
        BuildBroker()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var consumer = new EventConsumer();
        services.AddKeyedSingleton<IMessageConsumer>(nameof(EventConsumer), consumer);
        services.AddSingleton<MessageSerializerJson>();
        services.AddSingleton<IAggregator, AggregatorImpl>();
        var sp = services.BuildServiceProvider();

        var tap = new MemoryQueueEndPoint("Tap");
        var broker = new NymBrokerImpl(
            sp.GetRequiredService<MessageSerializerJson>(),
            sp.GetRequiredService<IAggregator>(),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance);

        broker.AddEndpoint("Tap", tap);
        broker.RegisterConsumer(typeof(Event), nameof(EventConsumer));
        broker.AddWireTap("Tap");

        return (broker, consumer, tap, sp.GetRequiredService<MessageSerializerJson>());
    }

    private static async Task<string> SerializeAsync<T>(MessageSerializerJson serializer, T message) where T : class
    {
        var ctx = new MessageContext<T> { Message = message };
        using var stream = serializer.Serialize(ctx);
        using var reader = new StreamReader(stream);
        return await reader.ReadToEndAsync();
    }

    [Fact]
    public async Task MessageCopiedToTap_AndConsumerStillReceives()
    {
        var (broker, consumer, tap, serializer) = BuildBroker();

        var json = await SerializeAsync(serializer, new Event { Name = "test" });
        await broker.ProcessAsync(json, "In");

        Assert.Single(consumer.Received);

        var tapItems = new List<string>();
        await foreach (var item in tap.ReadAsync()) tapItems.Add(item);
        Assert.Single(tapItems);
    }

    [Fact]
    public async Task MultipleTaps_AllReceiveCopy()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var consumer = new EventConsumer();
        services.AddKeyedSingleton<IMessageConsumer>(nameof(EventConsumer), consumer);
        services.AddSingleton<MessageSerializerJson>();
        services.AddSingleton<IAggregator, AggregatorImpl>();
        var sp = services.BuildServiceProvider();

        var tap1 = new MemoryQueueEndPoint("Tap1");
        var tap2 = new MemoryQueueEndPoint("Tap2");

        var broker = new NymBrokerImpl(
            sp.GetRequiredService<MessageSerializerJson>(),
            sp.GetRequiredService<IAggregator>(),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance);

        broker.AddEndpoint("Tap1", tap1);
        broker.AddEndpoint("Tap2", tap2);
        broker.RegisterConsumer(typeof(Event), nameof(EventConsumer));
        broker.AddWireTap("Tap1");
        broker.AddWireTap("Tap2");

        var json = await SerializeAsync(sp.GetRequiredService<MessageSerializerJson>(), new Event { Name = "x" });
        await broker.ProcessAsync(json, "In");

        var items1 = new List<string>();
        await foreach (var item in tap1.ReadAsync()) items1.Add(item);
        Assert.Single(items1);

        var items2 = new List<string>();
        await foreach (var item in tap2.ReadAsync()) items2.Add(item);
        Assert.Single(items2);
    }

    [Fact]
    public async Task TapContainsRawJsonPayload()
    {
        var (broker, _, tap, serializer) = BuildBroker();

        var json = await SerializeAsync(serializer, new Event { Name = "hello" });
        await broker.ProcessAsync(json, "In");

        var tapItems = new List<string>();
        await foreach (var item in tap.ReadAsync()) tapItems.Add(item);

        Assert.Contains("hello", tapItems[0]);
    }
}
