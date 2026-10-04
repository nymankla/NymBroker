using NymBroker.Core.Aggregator;
using NymBroker.Core.Consume;
using NymBroker.Core.Endpoint.File;
using NymBroker.Core.Filter;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.PubSub;
using NymBroker.Core.Serialize;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace NymBroker.Tests;

public sealed class MessageFlowTests
{
    private sealed class OrderMessage
    {
        public int Id { get; set; }
    }

    private sealed class OrderConsumer(TaskCompletionSource<OrderMessage>? delivered = null) : IConsume<OrderMessage>
    {
        public List<OrderMessage> Received { get; } = [];
        public IMessageContext? LastContext { get; private set; }

        public Task ConsumeAsync(OrderMessage message, IMessageContext context, CancellationToken ct = default)
        {
            Received.Add(message);
            LastContext = context;
            delivered?.TrySetResult(message);
            return Task.CompletedTask;
        }
    }

    private sealed class CallbackFilter(Func<IMessageContext, IMessageContext?> filter) : IMessageFilter
    {
        public IMessageContext? Filter(IMessageContext context) => filter(context);
    }

    private static (NymBrokerImpl Broker, ServiceProvider Provider) CreateBroker(
        string consumerKey, IMessageConsumer consumer)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKeyedSingleton<IMessageConsumer>(consumerKey, consumer);
        services.AddSingleton<MessageSerializerJson>();
        services.AddSingleton<IAggregator, AggregatorImpl>();
        var provider = services.BuildServiceProvider();

        var broker = new NymBrokerImpl(
            provider.GetRequiredService<MessageSerializerJson>(),
            provider.GetRequiredService<IAggregator>(),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance);
        broker.RegisterConsumer(typeof(OrderMessage), consumerKey);

        return (broker, provider);
    }

    private static async Task<string> SerializeAsync<T>(MessageSerializerJson serializer, T message) where T : class
    {
        using var stream = serializer.Serialize(new MessageContext<T> { Message = message });
        using var reader = new StreamReader(stream);
        return await reader.ReadToEndAsync();
    }

    [Fact]
    public async Task ProcessAsync_ChainedFiltersRunInOrder_BeforeConsumerDispatch()
    {
        var consumer = new OrderConsumer();
        var (broker, provider) = CreateBroker(nameof(OrderConsumer), consumer);
        await using var _ = provider;
        var expectedCorrelationId = Guid.NewGuid();
        var secondFilterObserved = Guid.Empty;

        broker.AddFilter(new CallbackFilter(context =>
        {
            context.CorrelationId = expectedCorrelationId;
            return context;
        }));
        broker.AddFilter(new CallbackFilter(context =>
        {
            secondFilterObserved = context.CorrelationId;
            return context;
        }));

        await broker.ProcessAsync(
            await SerializeAsync(provider.GetRequiredService<MessageSerializerJson>(), new OrderMessage { Id = 7 }),
            "Source",
            TestContext.Current.CancellationToken);

        Assert.Equal(expectedCorrelationId, secondFilterObserved);
        Assert.Equal(expectedCorrelationId, consumer.LastContext?.CorrelationId);
        Assert.Equal(7, Assert.Single(consumer.Received).Id);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public async Task ProcessAsync_EmptyOrWhitespacePayload_DoesNotDispatch(string payload)
    {
        var consumer = new OrderConsumer();
        var (broker, provider) = CreateBroker(nameof(OrderConsumer), consumer);
        await using var _ = provider;

        await broker.ProcessAsync(payload, "Source", TestContext.Current.CancellationToken);

        Assert.Empty(consumer.Received);
    }

    [Fact]
    public async Task FileEndpoint_ExistingMessageIsProcessedAndArchivedByBroker()
    {
        var root = Path.Combine(Path.GetTempPath(), $"nymbroker-file-flow-{Guid.NewGuid():N}");
        var readPath = Path.Combine(root, "in");
        var postPath = Path.Combine(root, "out");
        Directory.CreateDirectory(readPath);
        var delivered = new TaskCompletionSource<OrderMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new OrderConsumer(delivered);
        var (broker, provider) = CreateBroker(nameof(OrderConsumer), consumer);
        await using var _ = provider;

        try
        {
            var serializer = provider.GetRequiredService<MessageSerializerJson>();
            var inputFile = Path.Combine(readPath, "order.json");
            await File.WriteAllTextAsync(
                inputFile,
                await SerializeAsync(serializer, new OrderMessage { Id = 42 }),
                TestContext.Current.CancellationToken);

            var endpoint = new FileEndPoint(
                "FileIn",
                new FileSettings { ReadPath = readPath, PostPath = postPath, IsAbsolutePath = true },
                NullLogger<FileEndPoint>.Instance);
            broker.AddEndpoint("FileIn", endpoint);

            await broker.StartAsync(TestContext.Current.CancellationToken);
            var message = await delivered.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await broker.StopAsync(TestContext.Current.CancellationToken);

            Assert.Equal(42, message.Id);
            Assert.False(File.Exists(inputFile));
            Assert.True(File.Exists(Path.ChangeExtension(inputFile, ".processed")));
        }
        finally
        {
            await broker.StopAsync(TestContext.Current.CancellationToken);
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task FileEndpoint_EmptyFileCanBeProcessedAfterWriterCompletesIt()
    {
        var root = Path.Combine(Path.GetTempPath(), $"nymbroker-file-ready-{Guid.NewGuid():N}");
        var readPath = Path.Combine(root, "in");
        var postPath = Path.Combine(root, "out");
        Directory.CreateDirectory(readPath);

        try
        {
            var inputFile = Path.Combine(readPath, "order.json");
            await File.WriteAllTextAsync(inputFile, string.Empty, TestContext.Current.CancellationToken);
            var endpoint = new FileEndPoint(
                "FileIn",
                new FileSettings { ReadPath = readPath, PostPath = postPath, IsAbsolutePath = true },
                NullLogger<FileEndPoint>.Instance);

            var earlyReads = new List<string>();
            await foreach (var item in endpoint.ReadAsync(TestContext.Current.CancellationToken))
                earlyReads.Add(item);

            Assert.Empty(earlyReads);
            await File.WriteAllTextAsync(inputFile, """{"complete":true}""", TestContext.Current.CancellationToken);

            var completedReads = new List<string>();
            await foreach (var item in endpoint.ReadAsync(TestContext.Current.CancellationToken))
                completedReads.Add(item);

            Assert.Equal("""{"complete":true}""", Assert.Single(completedReads));
            Assert.True(File.Exists(Path.ChangeExtension(inputFile, ".processed")));
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
