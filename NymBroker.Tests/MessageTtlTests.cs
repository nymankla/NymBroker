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

public sealed class MessageTtlTests
{
    private sealed class Report { public int Id { get; set; } }

    private sealed class ReportConsumer : IConsume<Report>
    {
        public List<Report> Received { get; } = [];
        public Task ConsumeAsync(Report message, IMessageContext context, CancellationToken ct = default)
        {
            Received.Add(message);
            return Task.CompletedTask;
        }
    }

    private static ServiceProvider BuildServices(ReportConsumer consumer)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKeyedSingleton<IMessageConsumer>(nameof(ReportConsumer), consumer);
        services.AddSingleton<MessageSerializerJson>();
        services.AddSingleton<IAggregator, AggregatorImpl>();
        return services.BuildServiceProvider();
    }

    private static NymBrokerImpl BuildBroker(ServiceProvider sp, ReportConsumer consumer,
        TimeSpan maxAge, MemoryQueueEndPoint? dlq = null)
    {
        var broker = new NymBrokerImpl(
            sp.GetRequiredService<MessageSerializerJson>(),
            sp.GetRequiredService<IAggregator>(),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance);

        broker.RegisterConsumer(typeof(Report), nameof(ReportConsumer));
        broker.SetMaxMessageAge(maxAge);

        if (dlq != null)
        {
            broker.AddEndpoint("DLQ", dlq);
            broker.SetDeadLetterEndpoint("DLQ");
        }

        return broker;
    }

    private static async Task<string> SerializeAsync(MessageSerializerJson serializer, Report message,
        DateTime created)
    {
        var ctx = new MessageContext<Report> { Message = message, Created = created };
        using var stream = serializer.Serialize(ctx);
        using var reader = new StreamReader(stream);
        return await reader.ReadToEndAsync();
    }

    [Fact]
    public async Task ExpiredMessage_NotDeliveredToConsumer()
    {
        var consumer = new ReportConsumer();
        var sp = BuildServices(consumer);
        var broker = BuildBroker(sp, consumer, maxAge: TimeSpan.FromMinutes(1));

        var oldCreated = DateTime.UtcNow - TimeSpan.FromHours(2);
        var json = await SerializeAsync(sp.GetRequiredService<MessageSerializerJson>(),
            new Report { Id = 1 }, oldCreated);

        await broker.ProcessAsync(json, "In", TestContext.Current.CancellationToken);

        Assert.Empty(consumer.Received);
    }

    [Fact]
    public async Task FreshMessage_DeliveredToConsumer()
    {
        var consumer = new ReportConsumer();
        var sp = BuildServices(consumer);
        var broker = BuildBroker(sp, consumer, maxAge: TimeSpan.FromMinutes(5));

        var json = await SerializeAsync(sp.GetRequiredService<MessageSerializerJson>(),
            new Report { Id = 2 }, DateTime.UtcNow);

        await broker.ProcessAsync(json, "In", TestContext.Current.CancellationToken);

        Assert.Single(consumer.Received);
    }

    [Fact]
    public async Task ExpiredMessage_WithDlq_ForwardedToDeadLetter()
    {
        var consumer = new ReportConsumer();
        var sp = BuildServices(consumer);
        var dlq = new MemoryQueueEndPoint("DLQ");
        var broker = BuildBroker(sp, consumer, maxAge: TimeSpan.FromMinutes(1), dlq: dlq);

        var oldCreated = DateTime.UtcNow - TimeSpan.FromHours(2);
        var json = await SerializeAsync(sp.GetRequiredService<MessageSerializerJson>(),
            new Report { Id = 3 }, oldCreated);

        await broker.ProcessAsync(json, "In", TestContext.Current.CancellationToken);

        Assert.Empty(consumer.Received);

        var dlqItems = new List<string>();
        await foreach (var item in dlq.ReadAsync(TestContext.Current.CancellationToken)) dlqItems.Add(item);
        Assert.Single(dlqItems);
    }

    [Fact]
    public async Task ExpiredMessage_NoDlqConfigured_DoesNotThrow()
    {
        var consumer = new ReportConsumer();
        var sp = BuildServices(consumer);
        var broker = BuildBroker(sp, consumer, maxAge: TimeSpan.FromSeconds(1));

        var oldCreated = DateTime.UtcNow - TimeSpan.FromHours(1);
        var json = await SerializeAsync(sp.GetRequiredService<MessageSerializerJson>(),
            new Report { Id = 4 }, oldCreated);

        // Should not throw — expiry is silently discarded when no DLQ is configured.
        await broker.ProcessAsync(json, "In", TestContext.Current.CancellationToken);
        Assert.Empty(consumer.Received);
    }
}
