using NymBroker.Core.Aggregator;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Idempotency;
using NymBroker.Core.PubSub;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace NymBroker.Tests;

public sealed class IdempotentReceiverTests
{
    private sealed class Ping { public int Id { get; set; } }

    private sealed class PingConsumer : IConsume<Ping>
    {
        public List<Ping> Received { get; } = [];
        public Task ConsumeAsync(Ping message, IMessageContext context, CancellationToken ct = default)
        {
            Received.Add(message);
            return Task.CompletedTask;
        }
    }

    private static (NymBrokerImpl broker, PingConsumer consumer, MessageSerializerJson serializer)
        BuildBroker(TimeSpan? ttl = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var consumer = new PingConsumer();
        services.AddKeyedSingleton<IMessageConsumer>(nameof(PingConsumer), consumer);
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

        broker.RegisterConsumer(typeof(Ping), nameof(PingConsumer));

        var store = new InMemoryIdempotencyStore(ttl ?? TimeSpan.FromHours(1));
        broker.AddFilter(new IdempotentFilter(store));

        return (broker, consumer, sp.GetRequiredService<MessageSerializerJson>());
    }

    private static async Task<string> SerializeAsync<T>(MessageSerializerJson serializer, T message,
        Guid? id = null, DateTime? created = null) where T : class
    {
        var ctx = new MessageContext<T> { Message = message };
        if (created.HasValue) ctx.Created = created.Value;
        // Serialize with the generated Id — we cannot override it after construction, but that's fine.
        using var stream = serializer.Serialize(ctx);
        using var reader = new StreamReader(stream);
        return await reader.ReadToEndAsync();
    }

    [Fact]
    public async Task SameMessageTwice_ConsumerCalledOnlyOnce()
    {
        var (broker, consumer, serializer) = BuildBroker();

        var json = await SerializeAsync(serializer, new Ping { Id = 1 });

        await broker.ProcessAsync(json, "In", TestContext.Current.CancellationToken);
        await broker.ProcessAsync(json, "In", TestContext.Current.CancellationToken); // exact same JSON, same message Id

        Assert.Single(consumer.Received);
    }

    [Fact]
    public async Task DifferentMessages_BothDelivered()
    {
        var (broker, consumer, serializer) = BuildBroker();

        await broker.ProcessAsync(await SerializeAsync(serializer, new Ping { Id = 1 }), "In", TestContext.Current.CancellationToken);
        await broker.ProcessAsync(await SerializeAsync(serializer, new Ping { Id = 2 }), "In", TestContext.Current.CancellationToken);

        Assert.Equal(2, consumer.Received.Count);
    }

    [Fact]
    public async Task ExpiredEntry_AllowsReprocessing()
    {
        // TTL of 1 ms so the entry expires almost immediately.
        var (broker, consumer, serializer) = BuildBroker(ttl: TimeSpan.FromMilliseconds(1));

        var json = await SerializeAsync(serializer, new Ping { Id = 99 });

        await broker.ProcessAsync(json, "In", TestContext.Current.CancellationToken);

        // Wait for the TTL to expire.
        await Task.Delay(20, TestContext.Current.CancellationToken);

        await broker.ProcessAsync(json, "In", TestContext.Current.CancellationToken);

        Assert.Equal(2, consumer.Received.Count);
    }
}
