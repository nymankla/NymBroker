using System.Collections.Immutable;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Consume;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.PubSub;
using NymBroker.Core.Serialize;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace NymBroker.Tests;

public sealed class EndpointModeTests
{
    private sealed class OrderMessage { public string Id { get; set; } = string.Empty; }

    private sealed class TrackingEndPoint : IEndPointEventDriven
    {
        public EndpointMode Mode { get; }
        public int StartCalls { get; private set; }

        public TrackingEndPoint(EndpointMode mode = EndpointMode.ReadWrite)
        {
            Mode = mode;
        }

        public Task PostAsync(byte[] message, CancellationToken ct = default) => Task.CompletedTask;
        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();

        public Task StartListeningAsync(Func<byte[], CancellationToken, Task> handler, CancellationToken ct)
        {
            StartCalls++;
            return Task.CompletedTask;
        }

        public Task StopListeningAsync() => Task.CompletedTask;
    }

    private static NymBrokerImpl CreateBroker()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<MessageSerializerJson>();
        services.AddSingleton<IAggregator, AggregatorImpl>();
        var sp = services.BuildServiceProvider();

        return new NymBrokerImpl(
            sp.GetRequiredService<MessageSerializerJson>(),
            sp.GetRequiredService<IAggregator>(),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance);
    }

    // --- Mode property on endpoint ---

    [Theory]
    [InlineData(EndpointMode.ReadWrite)]
    [InlineData(EndpointMode.ReadOnly)]
    [InlineData(EndpointMode.WriteOnly)]
    public void MemoryEndPoint_Mode_ReturnsConfiguredValue(EndpointMode mode)
    {
        var ep = new MemoryQueueEndPoint("ep", mode: mode);
        Assert.Equal(mode, ep.Mode);
    }

    [Fact]
    public void MemoryEndPoint_DefaultMode_IsReadWrite()
    {
        var ep = new MemoryQueueEndPoint("ep");
        Assert.Equal(EndpointMode.ReadWrite, ep.Mode);
    }

    // --- Listener startup behaviour ---

    [Fact]
    public async Task StartAsync_ReadWriteEndpoint_StartsListener()
    {
        var broker = CreateBroker();
        var ep = new TrackingEndPoint(EndpointMode.ReadWrite);
        broker.AddEndpoint("rw", ep);

        await broker.StartAsync(TestContext.Current.CancellationToken);
        await broker.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(1, ep.StartCalls);
    }

    [Fact]
    public async Task StartAsync_ReadOnlyEndpoint_StartsListener()
    {
        var broker = CreateBroker();
        var ep = new TrackingEndPoint(EndpointMode.ReadOnly);
        broker.AddEndpoint("ro", ep);

        await broker.StartAsync(TestContext.Current.CancellationToken);
        await broker.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(1, ep.StartCalls);
    }

    [Fact]
    public async Task StartAsync_WriteOnlyEndpoint_DoesNotStartListener()
    {
        var broker = CreateBroker();
        var ep = new TrackingEndPoint(EndpointMode.WriteOnly);
        broker.AddEndpoint("wo", ep);

        await broker.StartAsync(TestContext.Current.CancellationToken);
        await broker.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(0, ep.StartCalls);
    }

    // --- PostAsync enforcement ---

    [Fact]
    public async Task PostAsync_ToReadOnlyEndpoint_ThrowsInvalidOperation()
    {
        var broker = CreateBroker();
        broker.AddEndpoint("ro", new MemoryQueueEndPoint("ro", mode: EndpointMode.ReadOnly));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => broker.PostAsync("ro", new OrderMessage { Id = "1" }, TestContext.Current.CancellationToken));

        Assert.Contains("ro", ex.Message);
        Assert.Contains("read-only", ex.Message);
    }

    [Fact]
    public async Task PostAsync_ToWriteOnlyEndpoint_Succeeds()
    {
        var broker = CreateBroker();
        broker.AddEndpoint("wo", new MemoryQueueEndPoint("wo", mode: EndpointMode.WriteOnly));

        // WriteOnly suppresses the listener; PostAsync must still work.
        await broker.PostAsync("wo", new OrderMessage { Id = "1" }, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task PostAsync_ToReadWriteEndpoint_Succeeds()
    {
        var broker = CreateBroker();
        broker.AddEndpoint("rw", new MemoryQueueEndPoint("rw"));

        await broker.PostAsync("rw", new OrderMessage { Id = "1" }, TestContext.Current.CancellationToken);
    }

    // --- Route validation at StartAsync ---

    [Fact]
    public async Task StartAsync_RouteTargetingReadOnlyEndpoint_Throws()
    {
        var broker = CreateBroker();
        broker.AddEndpoint("ro", new TrackingEndPoint(EndpointMode.ReadOnly));

        broker.Route<OrderMessage>().To("ro").Build();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => broker.StartAsync(TestContext.Current.CancellationToken));
        Assert.Contains("ro", ex.Message);
        Assert.Contains("read-only", ex.Message);
    }

    [Fact]
    public async Task StartAsync_RouteTargetingWriteOnlyEndpoint_Succeeds()
    {
        var broker = CreateBroker();
        broker.AddEndpoint("wo", new TrackingEndPoint(EndpointMode.WriteOnly));

        broker.Route<OrderMessage>().To("wo").Build();

        // WriteOnly endpoints are valid route destinations.
        await broker.StartAsync(TestContext.Current.CancellationToken);
        await broker.StopAsync(TestContext.Current.CancellationToken);
    }

    // --- Topic fan-out validation at StartAsync ---

    [Fact]
    public async Task StartAsync_TopicFanOutToReadOnlyEndpoint_Throws()
    {
        var broker = CreateBroker();
        broker.AddEndpoint("ro", new MemoryQueueEndPoint("ro", mode: EndpointMode.ReadOnly));

        broker.AddTopic(new TopicContext
        {
            TopicName = "orders",
            MessageType = typeof(OrderMessage),
            SubscriberEndpoints = ImmutableList.Create("ro")
        });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => broker.StartAsync(TestContext.Current.CancellationToken));
        Assert.Contains("ro", ex.Message);
        Assert.Contains("read-only", ex.Message);
    }

    [Fact]
    public async Task StartAsync_TopicFanOutToWriteOnlyEndpoint_Succeeds()
    {
        var broker = CreateBroker();
        broker.AddEndpoint("wo", new TrackingEndPoint(EndpointMode.WriteOnly));

        broker.AddTopic(new TopicContext
        {
            TopicName = "orders",
            MessageType = typeof(OrderMessage),
            SubscriberEndpoints = ImmutableList.Create("wo")
        });

        // WriteOnly endpoints are valid fan-out destinations.
        await broker.StartAsync(TestContext.Current.CancellationToken);
        await broker.StopAsync(TestContext.Current.CancellationToken);
    }
}
