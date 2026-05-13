using NymBroker.Core.Aggregator;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.PubSub;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace NymBroker.Tests;

public sealed class DeadLetterChannelTests
{
    private sealed class Order { public int Id { get; set; } }

    private sealed class ThrowingConsumer : IConsume<Order>
    {
        public Task ConsumeAsync(Order message, IMessageContext context, CancellationToken ct = default)
            => throw new InvalidOperationException("consumer failure");
    }

    private sealed class OrderConsumer : IConsume<Order>
    {
        public List<Order> Received { get; } = [];
        public Task ConsumeAsync(Order message, IMessageContext context, CancellationToken ct = default)
        {
            Received.Add(message);
            return Task.CompletedTask;
        }
    }

    private static (NymBrokerImpl broker, MemoryQueueEndPoint dlq, MessageSerializerJson serializer)
        BuildBroker(string consumerKey, IMessageConsumer consumer)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKeyedSingleton<IMessageConsumer>(consumerKey, consumer);
        services.AddSingleton<MessageSerializerJson>();
        services.AddSingleton<IAggregator, AggregatorImpl>();
        var sp = services.BuildServiceProvider();

        var dlq = new MemoryQueueEndPoint("DLQ");
        var broker = new NymBrokerImpl(
            sp.GetRequiredService<MessageSerializerJson>(),
            sp.GetRequiredService<IAggregator>(),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance);

        broker.AddEndpoint("DLQ", dlq);
        broker.RegisterConsumer(typeof(Order), consumerKey);
        broker.SetDeadLetterEndpoint("DLQ");

        return (broker, dlq, sp.GetRequiredService<MessageSerializerJson>());
    }

    private static async Task<string> SerializeAsync<T>(MessageSerializerJson serializer, T message) where T : class
    {
        var ctx = new MessageContext<T> { Message = message };
        using var stream = serializer.Serialize(ctx);
        using var reader = new StreamReader(stream);
        return await reader.ReadToEndAsync();
    }

    [Fact]
    public async Task ConsumerFailure_MessageForwardedToDeadLetter()
    {
        var (broker, dlq, serializer) = BuildBroker(nameof(ThrowingConsumer), new ThrowingConsumer());

        var json = await SerializeAsync(serializer, new Order { Id = 7 });
        await broker.ProcessAsync(json, "In");

        var items = new List<string>();
        await foreach (var item in dlq.ReadAsync()) items.Add(item);

        Assert.Single(items);
    }

    [Fact]
    public async Task SuccessfulConsumer_NothingSentToDeadLetter()
    {
        var consumer = new OrderConsumer();
        var (broker, dlq, serializer) = BuildBroker(nameof(OrderConsumer), consumer);

        var json = await SerializeAsync(serializer, new Order { Id = 1 });
        await broker.ProcessAsync(json, "In");

        Assert.Single(consumer.Received);

        var items = new List<string>();
        await foreach (var item in dlq.ReadAsync()) items.Add(item);
        Assert.Empty(items);
    }

    [Fact]
    public async Task DeadLetterMessage_ContainsOriginalPayload()
    {
        var (broker, dlq, serializer) = BuildBroker(nameof(ThrowingConsumer), new ThrowingConsumer());

        var json = await SerializeAsync(serializer, new Order { Id = 42 });
        await broker.ProcessAsync(json, "In");

        var items = new List<string>();
        await foreach (var item in dlq.ReadAsync()) items.Add(item);

        Assert.Contains("42", items[0]);
    }

    [Fact]
    public async Task WithoutDeadLetterEndpoint_ConsumerFailureDoesNotThrow()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKeyedSingleton<IMessageConsumer>(nameof(ThrowingConsumer), new ThrowingConsumer());
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
        broker.RegisterConsumer(typeof(Order), nameof(ThrowingConsumer));

        var serializer = sp.GetRequiredService<MessageSerializerJson>();
        var json = await SerializeAsync(serializer, new Order { Id = 1 });

        // Should not throw — failure is logged and swallowed.
        await broker.ProcessAsync(json, "In");
    }
}
