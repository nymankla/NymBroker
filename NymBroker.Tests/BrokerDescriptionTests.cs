using Microsoft.Extensions.DependencyInjection;
using NymBroker.Core;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.PubSub;
using NymBroker.Core.Scheduling;
using NymBroker.Core.Splitter;
using NymBroker.Endpoint.Sqlite;

namespace NymBroker.Tests;

public sealed class BrokerDescriptionTests
{
    [MessageName("describe.order")]
    public sealed record DescribeOrder(int Id);

    public sealed record DescribeInvoice(int Id);

    public sealed class OrderConsumer : IConsume<DescribeOrder>
    {
        public Task ConsumeAsync(DescribeOrder message, IMessageContext context, CancellationToken ct = default) => Task.CompletedTask;
    }

    public sealed class InvoiceConsumer : IConsume<DescribeInvoice>
    {
        public Task ConsumeAsync(DescribeInvoice message, IMessageContext context, CancellationToken ct = default) => Task.CompletedTask;
    }

    public sealed class AuditSubscriber : ISubscribe<DescribeOrder>
    {
        public Task ReceiveAsync(DescribeOrder message, IMessageContext context, CancellationToken ct = default) => Task.CompletedTask;
    }

    private static async Task<(ServiceProvider Services, INymBroker Broker)> BuildAsync()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<ICompressor, ZstandardCompressor>();
        services.AddNymBroker()
            .AddMemoryEndPoint("In")
            .AddMemoryEndPoint("Archive", mode: EndpointMode.WriteOnly)
            .AddMemoryEndPoint("Dlq", mode: EndpointMode.WriteOnly)
            .AddMemoryEndPoint("Tap", mode: EndpointMode.WriteOnly)
            .AddSqliteEndPoint("Queue", new SqliteSettings { ConnectionString = "Data Source=describe;Mode=Memory;Cache=Shared" })
            .AddConsumer<OrderConsumer>()
            .AddConsumer<InvoiceConsumer>(o => o.RetryOnFailure = false)
            .AddRoute<DescribeOrder>(r => r.To("Archive").WhenFrom("In").When(e => e.GetProperty("Id").GetInt32() > 1).WhenMessageIsOlderThan(TimeSpan.FromMinutes(5)))
            .AddTopic<DescribeOrder>("orders.events").SubscribeTo("Archive").SubscribeWith<AuditSubscriber>().Build()
            .AddScheduledAction(TimeSpan.FromMinutes(1), (_, _) => Task.CompletedTask)
            .AddScheduledAction("0 3 * * *", (_, _) => Task.CompletedTask)
            .AddScheduledAction("nightly-report", "0 2 * * *", (_, _) => Task.CompletedTask, o => o.MissedRuns = MissedRunPolicy.RunOnce)
            .AddScheduledAction("tick", TimeSpan.FromSeconds(30), (_, _) => Task.CompletedTask)
            .WithDeadLetterEndpoint("Dlq")
            .AddWireTap("Tap")
            .DiscardMessagesOlderThan(TimeSpan.FromHours(1))
            .AddIdempotentReceiver(TimeSpan.FromMinutes(10))
            .ConfigureHealthCheck(o => o.NonCritical("Archive"))
            .Build();
        var sp = services.BuildServiceProvider();
        return (sp, sp.GetRequiredService<INymBroker>());
    }

    [Fact]
    public async Task Describe_ListsEndpointsWithTheirCapabilities()
    {
        var (sp, broker) = await BuildAsync();
        await using var _ = sp;

        var endpoints = broker.Describe().Endpoints.ToDictionary(e => e.Name);

        Assert.Equal(["Archive", "Dlq", "In", "Queue", "Tap"], endpoints.Keys.Order(StringComparer.Ordinal));
        Assert.Equal("Memory", endpoints["In"].Kind);
        Assert.True(endpoints["In"].Listens);
        Assert.False(endpoints["Archive"].Listens);
        Assert.False(endpoints["Archive"].IsCritical);
        Assert.True(endpoints["In"].IsCritical);

        var queue = endpoints["Queue"];
        Assert.Equal("Sqlite", queue.Kind);
        Assert.True(queue.Inspectable);
        Assert.True(queue.CanDefer);
        Assert.True(queue.UsesNativeDeadLetter);
        Assert.False(endpoints["In"].Inspectable);
    }

    [Fact]
    public async Task Describe_ListsRoutesTopicsAndConsumers_ByWireName()
    {
        var (sp, broker) = await BuildAsync();
        await using var _ = sp;

        var description = broker.Describe();

        var route = Assert.Single(description.Routes);
        Assert.Equal("describe.order", route.MessageType);
        Assert.Equal("Archive", route.Destination);
        Assert.Equal("In", route.From);
        Assert.Equal("predicate and older than 00:05:00", route.Condition);

        var topic = Assert.Single(description.Topics);
        Assert.Equal("orders.events", topic.Name);
        Assert.Equal(["Archive"], topic.SubscriberEndpoints);
        Assert.Equal([nameof(AuditSubscriber)], topic.Subscribers);

        Assert.Contains(description.Consumers, c => c is { MessageType: "describe.order", Consumer: nameof(OrderConsumer), RetryOnFailure: true });
        Assert.Contains(description.Consumers, c => c.Consumer == nameof(InvoiceConsumer) && !c.RetryOnFailure);
    }

    [Fact]
    public async Task Describe_ListsScheduledActions_WithTheNextRunWhereItIsKnown()
    {
        var (sp, broker) = await BuildAsync();
        await using var _ = sp;
        var before = DateTimeOffset.Now;

        var actions = broker.Describe().ScheduledActions;

        Assert.Equal(4, actions.Count);
        var interval = Assert.Single(actions, a => a is { Name: null, Schedule: "every 00:01:00" });
        Assert.Null(interval.NextRun);
        var cron = Assert.Single(actions, a => a is { Name: null, Schedule: "0 3 * * *" });
        Assert.True(cron.NextRun > before);
        var nightly = Assert.Single(actions, a => a.Name == "nightly-report");
        Assert.Equal("0 2 * * *", nightly.Schedule);
        Assert.Equal(MissedRunPolicy.RunOnce, nightly.MissedRuns);
        Assert.True(nightly.NextRun > before);
        var tick = Assert.Single(actions, a => a.Name == "tick");
        Assert.Equal("every 00:00:30", tick.Schedule);
        Assert.True(tick.NextRun > before && tick.NextRun <= before.AddSeconds(31));
    }

    [Fact]
    public async Task Describe_ReportsBrokerWideFeatures_AndWhetherItHasStarted()
    {
        var (sp, broker) = await BuildAsync();
        await using var _ = sp;

        var features = broker.Describe().Features;
        Assert.Equal("Dlq", features.DeadLetterEndpoint);
        Assert.Equal(["Tap"], features.WireTaps);
        Assert.Equal(TimeSpan.FromHours(1), features.MaxMessageAge);
        Assert.NotNull(features.IdempotencyStore);
        Assert.Equal("zstd", features.Compressor);
        Assert.False(broker.Describe().Started);

        await broker.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            Assert.True(broker.Describe().Started);
        }
        finally
        {
            await broker.StopAsync(TestContext.Current.CancellationToken);
        }
        Assert.False(broker.Describe().Started);
    }

    [Theory]
    [InlineData(typeof(MemoryQueueEndPoint), "Memory")]
    [InlineData(typeof(SqliteEndPoint), "Sqlite")]
    [InlineData(typeof(BrokerDescriptionTests), "BrokerDescriptionTests")]
    public void EndpointKind_DropsTheEndPointSuffix(Type type, string expected)
    {
        Assert.Equal(expected, NymBrokerImpl.EndpointKind(type));
    }

    [Fact]
    public void Describe_OnACustomBroker_IsNotSupported()
    {
        INymBroker custom = new CustomBroker();
        Assert.Throws<NotSupportedException>(() => custom.Describe());
    }

    private sealed class CustomBroker : INymBroker
    {
        public Task PostAsync<T>(string endpointName, T message, CancellationToken ct = default, int? splitThresholdBytes = null, bool compress = true) where T : class => throw new NotImplementedException();
        public Task PostRawAsync(string endpointName, Stream message, CancellationToken ct = default, int? splitThresholdBytes = null, bool compress = true) => throw new NotImplementedException();
        public Task PostRawAsync(string endpointName, ReadOnlyMemory<byte> message, CancellationToken ct = default, int? splitThresholdBytes = null, bool compress = true) => throw new NotImplementedException();
        public Task PostDeferredAsync<T>(string endpointName, T message, DateTimeOffset deliverAt, CancellationToken ct = default) where T : class => throw new NotImplementedException();
        public Task PostRawDeferredAsync(string endpointName, Stream message, DateTimeOffset deliverAt, CancellationToken ct = default) => throw new NotImplementedException();
        public Task PostRawDeferredAsync(string endpointName, ReadOnlyMemory<byte> message, DateTimeOffset deliverAt, CancellationToken ct = default) => throw new NotImplementedException();
        public Task PostBatchAsync<T>(string endpointName, IEnumerable<T> messages, CancellationToken ct = default, int? splitThresholdBytes = null, bool compress = true, Guid? correlationId = null) where T : class => throw new NotImplementedException();
        public Task PublishBatchAsync<T>(IEnumerable<T> messages, CancellationToken ct = default) where T : class => throw new NotImplementedException();
        public Task PublishBatchAsync<T>(string topicName, IEnumerable<T> messages, CancellationToken ct = default) where T : class => throw new NotImplementedException();
        public Task PublishAsync<T>(T message, CancellationToken ct = default) where T : class => throw new NotImplementedException();
        public Task PublishAsync<T>(string topicName, T message, CancellationToken ct = default) where T : class => throw new NotImplementedException();
        public Task<ProcessResult> ProcessAsync(string raw, string? sourceEndpoint = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<ProcessResult> ProcessAsync(byte[] raw, string? sourceEndpoint = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task StartAsync(CancellationToken ct = default) => throw new NotImplementedException();
        public Task StopAsync(CancellationToken ct = default) => throw new NotImplementedException();
        public Task<Core.Endpoint.HealthCheck.BrokerHealthReport> CheckHealthAsync(CancellationToken ct = default) => throw new NotImplementedException();
    }
}
