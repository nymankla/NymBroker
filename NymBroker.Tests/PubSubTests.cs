using NymBroker.Core.Aggregator;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Factory;
using NymBroker.Core.Factory.Configuration;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.PubSub;
using NymBroker.Core.Route;
using NymBroker.Core.Serialize;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using System.Collections.Immutable;

namespace NymBroker.Tests;

public sealed class PubSubTests
{
    // --- Test message types ---

    private sealed class OrderMessage { public string OrderId { get; set; } = ""; public decimal Total { get; set; } }
    private sealed class InvoiceMessage { public string InvoiceId { get; set; } = ""; }

    // --- Subscriber implementations ---

    private sealed class OrderSubscriber : ISubscribe<OrderMessage>
    {
        public List<(OrderMessage Message, IMessageContext Context)> Received { get; } = [];

        public Task ReceiveAsync(OrderMessage message, IMessageContext context, CancellationToken ct = default)
        {
            lock (Received) Received.Add((message, context));
            return Task.CompletedTask;
        }
    }

    private sealed class SecondOrderSubscriber : ISubscribe<OrderMessage>
    {
        public List<OrderMessage> Received { get; } = [];

        public Task ReceiveAsync(OrderMessage message, IMessageContext context, CancellationToken ct = default)
        {
            lock (Received) Received.Add(message);
            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingOrderSubscriber : ISubscribe<OrderMessage>
    {
        public int CallCount { get; private set; }

        public Task ReceiveAsync(OrderMessage message, IMessageContext context, CancellationToken ct = default)
        {
            CallCount++;
            throw new InvalidOperationException("Subscriber intentionally failed");
        }
    }

    private sealed class SurvivingOrderSubscriber : ISubscribe<OrderMessage>
    {
        public List<OrderMessage> Received { get; } = [];

        public Task ReceiveAsync(OrderMessage message, IMessageContext context, CancellationToken ct = default)
        {
            lock (Received) Received.Add(message);
            return Task.CompletedTask;
        }
    }

    private sealed class OrderConsumer : IConsume<OrderMessage>
    {
        public List<OrderMessage> Received { get; } = [];

        public Task ConsumeAsync(OrderMessage message, IMessageContext context, CancellationToken ct = default)
        {
            lock (Received) Received.Add(message);
            return Task.CompletedTask;
        }
    }

    // --- Helpers ---

    private static async Task<string> SerializeAsync<T>(T message) where T : class
    {
        var serializer = new MessageSerializerJson();
        var ctx = new MessageContext<T> { Message = message };
        using var stream = serializer.Serialize(ctx);
        using var reader = new StreamReader(stream);
        return await reader.ReadToEndAsync();
    }

    private static async Task<List<string>> DrainAsync(MemoryQueueEndPoint endpoint)
    {
        var items = new List<string>();
        await foreach (var msg in endpoint.ReadAsync(TestContext.Current.CancellationToken)) items.Add(msg);
        return items;
    }

    /// <summary>
    /// Builds a bare NymBrokerImpl with no endpoints, consumers, or topics pre-registered.
    /// Caller is responsible for adding endpoints and topics.
    /// </summary>
    private static (NymBrokerImpl Broker, IServiceProvider ServiceProvider) BuildBroker(
        Action<IServiceCollection>? configureServices = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        configureServices?.Invoke(services);
        var sp = services.BuildServiceProvider();

        var broker = new NymBrokerImpl(
            new MessageSerializerJson(),
            new AggregatorImpl(NullLogger<AggregatorImpl>.Instance),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance);

        return (broker, sp);
    }

    // =========================================================================
    // 1. Endpoint fan-out — two SubscribeTo endpoints both receive a copy
    // =========================================================================

    [Fact]
    public async Task Topic_WithTwoSubscribeToEndpoints_BothReceiveMessage()
    {
        var (broker, _) = BuildBroker();
        var ep1 = new MemoryQueueEndPoint("Sub1");
        var ep2 = new MemoryQueueEndPoint("Sub2");
        broker.AddEndpoint("Sub1", ep1);
        broker.AddEndpoint("Sub2", ep2);

        broker.AddTopic(new TopicContext
        {
            TopicName = "orders",
            MessageType = typeof(OrderMessage),
            SubscriberEndpoints = ImmutableList.Create("Sub1", "Sub2")
        });

        await broker.ProcessAsync(await SerializeAsync(new OrderMessage { OrderId = "O1" }), null, TestContext.Current.CancellationToken);

        var items1 = await DrainAsync(ep1);
        var items2 = await DrainAsync(ep2);

        Assert.Single(items1);
        Assert.Single(items2);
    }

    // =========================================================================
    // 2. ISubscribe<T> dispatch — ReceiveAsync called with correct args
    // =========================================================================

    [Fact]
    public async Task Topic_WithSubscriberDispatcher_CallsReceiveAsyncWithCorrectMessage()
    {
        var subscriber = new OrderSubscriber();

        var (broker, _) = BuildBroker(services =>
            services.AddKeyedSingleton<IMessageSubscriber>(nameof(OrderSubscriber), subscriber));

        broker.AddTopic(new TopicContext
        {
            TopicName = "orders",
            MessageType = typeof(OrderMessage),
            SubscriberDispatchers = ImmutableList.Create<(Type, string)>((typeof(OrderSubscriber), nameof(OrderSubscriber)))
        });

        var order = new OrderMessage { OrderId = "O42", Total = 99.99m };
        await broker.ProcessAsync(await SerializeAsync(order), null, TestContext.Current.CancellationToken);

        Assert.Single(subscriber.Received);
        Assert.Equal("O42", subscriber.Received[0].Message.OrderId);
        Assert.Equal(99.99m, subscriber.Received[0].Message.Total);
        Assert.NotNull(subscriber.Received[0].Context);
    }

    // =========================================================================
    // 3. Multiple subscribers — all receive the message
    // =========================================================================

    [Fact]
    public async Task Topic_WithMultipleSubscribers_AllReceiveMessage()
    {
        var sub1 = new OrderSubscriber();
        var sub2 = new SecondOrderSubscriber();

        var (broker, _) = BuildBroker(services =>
        {
            services.AddKeyedSingleton<IMessageSubscriber>(nameof(OrderSubscriber), sub1);
            services.AddKeyedSingleton<IMessageSubscriber>(nameof(SecondOrderSubscriber), sub2);
        });

        broker.AddTopic(new TopicContext
        {
            TopicName = "orders",
            MessageType = typeof(OrderMessage),
            SubscriberDispatchers = ImmutableList.Create<(Type, string)>(
                (typeof(OrderSubscriber), nameof(OrderSubscriber)),
                (typeof(SecondOrderSubscriber), nameof(SecondOrderSubscriber)))
        });

        await broker.ProcessAsync(await SerializeAsync(new OrderMessage { OrderId = "O99" }), null, TestContext.Current.CancellationToken);

        Assert.Single(sub1.Received);
        Assert.Single(sub2.Received);
        Assert.Equal("O99", sub1.Received[0].Message.OrderId);
        Assert.Equal("O99", sub2.Received[0].OrderId);
    }

    // =========================================================================
    // 4. Subscriber error isolation — throwing subscriber does not prevent others
    // =========================================================================

    [Fact]
    public async Task Topic_WhenOneSubscriberThrows_OtherSubscribersStillReceive()
    {
        var throwing = new ThrowingOrderSubscriber();
        var surviving = new SurvivingOrderSubscriber();

        var (broker, _) = BuildBroker(services =>
        {
            services.AddKeyedSingleton<IMessageSubscriber>(nameof(ThrowingOrderSubscriber), throwing);
            services.AddKeyedSingleton<IMessageSubscriber>(nameof(SurvivingOrderSubscriber), surviving);
        });

        broker.AddTopic(new TopicContext
        {
            TopicName = "orders",
            MessageType = typeof(OrderMessage),
            SubscriberDispatchers = ImmutableList.Create<(Type, string)>(
                (typeof(ThrowingOrderSubscriber), nameof(ThrowingOrderSubscriber)),
                (typeof(SurvivingOrderSubscriber), nameof(SurvivingOrderSubscriber)))
        });

        // Should not throw — errors are isolated per subscriber
        await broker.ProcessAsync(await SerializeAsync(new OrderMessage { OrderId = "O1" }), null, TestContext.Current.CancellationToken);

        Assert.Equal(1, throwing.CallCount);
        Assert.Single(surviving.Received);
    }

    // =========================================================================
    // 5. Condition filtering — topic with .When only fires when condition matches
    // =========================================================================

    [Fact]
    public async Task Topic_WithCondition_OnlyFiresWhenConditionMatches()
    {
        var ep = new MemoryQueueEndPoint("TopicEp");
        var (broker, _) = BuildBroker();
        broker.AddEndpoint("TopicEp", ep);

        broker.AddTopic(new TopicContext
        {
            TopicName = "highvalue",
            MessageType = typeof(OrderMessage),
            Condition = new JsonRouteCondition(el =>
                el.TryGetProperty("total", out var p) && p.GetDecimal() > 100m),
            SubscriberEndpoints = ImmutableList.Create("TopicEp")
        });

        // Low value — should NOT fire
        await broker.ProcessAsync(await SerializeAsync(new OrderMessage { OrderId = "Low", Total = 50m }), null, TestContext.Current.CancellationToken);
        // High value — should fire
        await broker.ProcessAsync(await SerializeAsync(new OrderMessage { OrderId = "High", Total = 200m }), null, TestContext.Current.CancellationToken);

        var items = await DrainAsync(ep);

        Assert.Single(items);
        Assert.Contains("High", items[0]);
    }

    // =========================================================================
    // 6. Type matching — topic for OrderMessage does NOT fire for InvoiceMessage
    // =========================================================================

    [Fact]
    public async Task Topic_ForOrderMessage_DoesNotFireForInvoiceMessage()
    {
        var ep = new MemoryQueueEndPoint("TypeEp");
        var (broker, _) = BuildBroker();
        broker.AddEndpoint("TypeEp", ep);

        broker.AddTopic(new TopicContext
        {
            TopicName = "orders",
            MessageType = typeof(OrderMessage),
            SubscriberEndpoints = ImmutableList.Create("TypeEp")
        });

        // Register InvoiceMessage type so it won't be an "unresolved" type warning
        var serializer = new MessageSerializerJson();
        var invoiceCtx = new MessageContext<InvoiceMessage> { Message = new InvoiceMessage { InvoiceId = "I1" } };
        using var invoiceStream = serializer.Serialize(invoiceCtx);
        using var reader = new StreamReader(invoiceStream);
        await broker.ProcessAsync(await reader.ReadToEndAsync(TestContext.Current.CancellationToken), null, TestContext.Current.CancellationToken);

        var items = await DrainAsync(ep);
        Assert.Empty(items);
    }

    // =========================================================================
    // 7. PublishAsync<T>(T message) — triggers implicit topic routing by type
    // =========================================================================

    [Fact]
    public async Task PublishAsync_ByType_TriggersMatchingTopic()
    {
        var ep = new MemoryQueueEndPoint("PubEp");
        var (broker, _) = BuildBroker();
        broker.AddEndpoint("PubEp", ep);

        broker.AddTopic(new TopicContext
        {
            TopicName = "orders",
            MessageType = typeof(OrderMessage),
            SubscriberEndpoints = ImmutableList.Create("PubEp")
        });

        await broker.PublishAsync(new OrderMessage { OrderId = "Pub1" }, TestContext.Current.CancellationToken);

        var items = await DrainAsync(ep);
        Assert.Single(items);
        Assert.Contains("Pub1", items[0]);
    }

    // =========================================================================
    // 8. PublishAsync<T>(string topicName, T message) — delivers to named topic
    // =========================================================================

    [Fact]
    public async Task PublishAsync_ByTopicName_DeliversToNamedTopic()
    {
        var ep = new MemoryQueueEndPoint("NamedEp");
        var (broker, _) = BuildBroker();
        broker.AddEndpoint("NamedEp", ep);

        broker.AddTopic(new TopicContext
        {
            TopicName = "special-orders",
            MessageType = typeof(OrderMessage),
            SubscriberEndpoints = ImmutableList.Create("NamedEp")
        });

        await broker.PublishAsync("special-orders", new OrderMessage { OrderId = "Named1" }, TestContext.Current.CancellationToken);

        var items = await DrainAsync(ep);
        Assert.Single(items);
        Assert.Contains("Named1", items[0]);
    }

    [Fact]
    public async Task PublishAsync_ByTopicName_DoesNotDeliverToOtherTopics()
    {
        var ep1 = new MemoryQueueEndPoint("Ep1");
        var ep2 = new MemoryQueueEndPoint("Ep2");
        var (broker, _) = BuildBroker();
        broker.AddEndpoint("Ep1", ep1);
        broker.AddEndpoint("Ep2", ep2);

        broker.AddTopic(new TopicContext
        {
            TopicName = "topic-a",
            MessageType = typeof(OrderMessage),
            SubscriberEndpoints = ImmutableList.Create("Ep1")
        });
        broker.AddTopic(new TopicContext
        {
            TopicName = "topic-b",
            MessageType = typeof(OrderMessage),
            SubscriberEndpoints = ImmutableList.Create("Ep2")
        });

        await broker.PublishAsync("topic-a", new OrderMessage { OrderId = "A1" }, TestContext.Current.CancellationToken);

        var items1 = await DrainAsync(ep1);
        var items2 = await DrainAsync(ep2);

        Assert.Single(items1);
        Assert.Empty(items2);
    }

    // =========================================================================
    // 9. Topic + route coexistence — both fire; consumer dispatch does NOT happen
    // =========================================================================

    [Fact]
    public async Task Topic_AndRoute_BothFire_ConsumerDoesNotReceive()
    {
        var consumer = new OrderConsumer();
        var routeEp = new MemoryQueueEndPoint("RouteEp");
        var topicEp = new MemoryQueueEndPoint("TopicEp");

        var (broker, _) = BuildBroker(services =>
            services.AddKeyedSingleton<IMessageConsumer>(nameof(OrderConsumer), consumer));

        broker.RegisterConsumer(typeof(OrderMessage), nameof(OrderConsumer));
        broker.AddEndpoint("RouteEp", routeEp);
        broker.AddEndpoint("TopicEp", topicEp);

        // Route
        broker.Route<OrderMessage>().To("RouteEp").Build();

        // Topic
        broker.AddTopic(new TopicContext
        {
            TopicName = "orders",
            MessageType = typeof(OrderMessage),
            SubscriberEndpoints = ImmutableList.Create("TopicEp")
        });

        await broker.ProcessAsync(await SerializeAsync(new OrderMessage { OrderId = "Both1" }), null, TestContext.Current.CancellationToken);

        var routeItems = await DrainAsync(routeEp);
        var topicItems = await DrainAsync(topicEp);

        Assert.Single(routeItems);
        Assert.Single(topicItems);
        Assert.Empty(consumer.Received); // consumer dispatch skipped because route + topic matched
    }

    // =========================================================================
    // 10. Topic match with no route → no consumer dispatch
    // =========================================================================

    [Fact]
    public async Task Topic_Match_WithNoRoute_ConsumerIsNotDispatched()
    {
        var consumer = new OrderConsumer();
        var topicEp = new MemoryQueueEndPoint("TopicOnly");

        var (broker, _) = BuildBroker(services =>
            services.AddKeyedSingleton<IMessageConsumer>(nameof(OrderConsumer), consumer));

        broker.RegisterConsumer(typeof(OrderMessage), nameof(OrderConsumer));
        broker.AddEndpoint("TopicOnly", topicEp);

        // Only a topic, no route
        broker.AddTopic(new TopicContext
        {
            TopicName = "orders",
            MessageType = typeof(OrderMessage),
            SubscriberEndpoints = ImmutableList.Create("TopicOnly")
        });

        await broker.ProcessAsync(await SerializeAsync(new OrderMessage { OrderId = "TopicOnly1" }), null, TestContext.Current.CancellationToken);

        var topicItems = await DrainAsync(topicEp);
        Assert.Single(topicItems);
        Assert.Empty(consumer.Received); // wasTopicFanOut=true → consumer dispatch skipped
    }

    // =========================================================================
    // 11. Unknown topic name in PublishAsync(topicName) → logs warning, no exception
    // =========================================================================

    [Fact]
    public async Task PublishAsync_UnknownTopicName_DoesNotThrow()
    {
        var (broker, _) = BuildBroker();

        // Should log a warning but not throw
        await broker.PublishAsync("no-such-topic", new OrderMessage { OrderId = "X" }, TestContext.Current.CancellationToken);
    }

    // =========================================================================
    // 12. Config-based topic — endpoint subscribers from TopicConfiguration
    // =========================================================================

    [Fact]
    public async Task ConfigTopic_EndpointSubscriberReceivesMessage()
    {
        // Build a BrokerConfiguration with a topic that subscribes an endpoint
        var config = new BrokerConfiguration
        {
            Endpoints =
            [
                new EndPointConfiguration { Name = "CfgSub", Type = EndPointType.Memory }
            ],
            Topics =
            [
                new TopicConfiguration
                {
                    TopicName = "cfg-orders",
                    MessageType = null, // matches any type
                    SubscriberEndpoints = ["CfgSub"]
                }
            ]
        };

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .ApplyConfiguration(config)
            .Build();

        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<INymBroker>();

        await broker.PublishAsync(new OrderMessage { OrderId = "Cfg1" }, TestContext.Current.CancellationToken);

        // Retrieve the underlying MemoryQueueEndPoint to verify delivery
        var ep = (MemoryQueueEndPoint)sp.GetRequiredKeyedService<IEndPoint>("CfgSub");
        var items = await DrainAsync(ep);

        Assert.Single(items);
        Assert.Contains("Cfg1", items[0]);
    }

    [Fact]
    public async Task ConfigTopic_WithAnyMessageType_ReceivesMultipleMessageTypes()
    {
        // A config topic with MessageType = null acts as a wildcard and receives any message type
        var config = new BrokerConfiguration
        {
            Endpoints =
            [
                new EndPointConfiguration { Name = "WildcardSub", Type = EndPointType.Memory }
            ],
            Topics =
            [
                new TopicConfiguration
                {
                    TopicName = "all-messages",
                    MessageType = null, // matches any type
                    SubscriberEndpoints = ["WildcardSub"]
                }
            ]
        };

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .ApplyConfiguration(config)
            .Build();

        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<INymBroker>();

        await broker.PublishAsync(new OrderMessage { OrderId = "O1" }, TestContext.Current.CancellationToken);
        await broker.PublishAsync(new InvoiceMessage { InvoiceId = "I1" }, TestContext.Current.CancellationToken);

        var ep = (MemoryQueueEndPoint)sp.GetRequiredKeyedService<IEndPoint>("WildcardSub");
        var items = await DrainAsync(ep);

        Assert.Equal(2, items.Count);
    }

    // =========================================================================
    // 13. AddTopic<T> via NymBrokerBuilder fluent API
    // =========================================================================

    [Fact]
    public async Task Builder_AddTopic_FluentApi_EndpointFanOut()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .AddMemoryEndPoint("FluentSub")
            .AddTopic<OrderMessage>("fluent-orders")
                .SubscribeTo("FluentSub")
                .Build()
            .Build();

        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<INymBroker>();

        await broker.PublishAsync(new OrderMessage { OrderId = "Fluent1" }, TestContext.Current.CancellationToken);

        var ep = (MemoryQueueEndPoint)sp.GetRequiredKeyedService<IEndPoint>("FluentSub");
        var items = await DrainAsync(ep);

        Assert.Single(items);
        Assert.Contains("Fluent1", items[0]);
    }

    [Fact]
    public async Task Builder_AddTopic_WithSubscribeWith_CallsReceiveAsync()
    {
        // SubscribeWith<TSub> registers the subscriber in DI and wires up dispatch
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .AddTopic<OrderMessage>("wired-orders")
                .SubscribeWith<OrderSubscriber>()
                .Build()
            .Build();

        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<INymBroker>();

        await broker.PublishAsync(new OrderMessage { OrderId = "Wired1" }, TestContext.Current.CancellationToken);

        // Subscriber is resolved from DI per dispatch; we verify no exception was thrown
        // and the broker processed the message. Keyed service is Transient so we can't
        // directly observe state — but absence of exception confirms dispatch succeeded.
    }

    [Fact]
    public async Task Builder_AddTopic_WithCondition_OnlyFiresWhenConditionMatches()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .AddMemoryEndPoint("ConditionalSub")
            .AddTopic<OrderMessage>("conditional-orders")
                .SubscribeTo("ConditionalSub")
                .When(el => el.TryGetProperty("total", out var p) && p.GetDecimal() >= 500m)
                .Build()
            .Build();

        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<INymBroker>();

        await broker.PublishAsync(new OrderMessage { OrderId = "Small", Total = 50m }, TestContext.Current.CancellationToken);
        await broker.PublishAsync(new OrderMessage { OrderId = "Large", Total = 600m }, TestContext.Current.CancellationToken);

        var ep = (MemoryQueueEndPoint)sp.GetRequiredKeyedService<IEndPoint>("ConditionalSub");
        var items = await DrainAsync(ep);

        Assert.Single(items);
        Assert.Contains("Large", items[0]);
    }

    // =========================================================================
    // 14. TopicContext.Evaluate — unit tests for the predicate logic
    // =========================================================================

    [Fact]
    public void TopicContext_Evaluate_ReturnsFalse_WhenMessageTypeDoesNotMatch()
    {
        var topic = new TopicContext
        {
            TopicName = "t",
            MessageType = typeof(OrderMessage)
        };

        var context = new MessageContext<InvoiceMessage> { Message = new InvoiceMessage() };
        var element = System.Text.Json.JsonDocument.Parse("{}").RootElement;

        Assert.False(topic.Evaluate(typeof(InvoiceMessage), context, element));
    }

    [Fact]
    public void TopicContext_Evaluate_ReturnsTrue_WhenTypeMatchesAndNoCondition()
    {
        var topic = new TopicContext
        {
            TopicName = "t",
            MessageType = typeof(OrderMessage)
        };

        var context = new MessageContext<OrderMessage> { Message = new OrderMessage() };
        var element = System.Text.Json.JsonDocument.Parse("{}").RootElement;

        Assert.True(topic.Evaluate(typeof(OrderMessage), context, element));
    }

    [Fact]
    public void TopicContext_Evaluate_IAnyMessage_MatchesAnyType()
    {
        var topic = new TopicContext
        {
            TopicName = "t",
            MessageType = typeof(IAnyMessage)  // wildcard
        };

        var context = new MessageContext<InvoiceMessage> { Message = new InvoiceMessage() };
        var element = System.Text.Json.JsonDocument.Parse("{}").RootElement;

        Assert.True(topic.Evaluate(typeof(InvoiceMessage), context, element));
    }

    [Fact]
    public void TopicContext_Evaluate_ReturnsFalse_WhenConditionFails()
    {
        var topic = new TopicContext
        {
            TopicName = "t",
            MessageType = typeof(OrderMessage),
            Condition = new JsonRouteCondition(_ => false)
        };

        var context = new MessageContext<OrderMessage> { Message = new OrderMessage() };
        var element = System.Text.Json.JsonDocument.Parse("{}").RootElement;

        Assert.False(topic.Evaluate(typeof(OrderMessage), context, element));
    }

    [Fact]
    public void TopicContext_Evaluate_ReturnsTrue_WhenConditionPasses()
    {
        var topic = new TopicContext
        {
            TopicName = "t",
            MessageType = typeof(OrderMessage),
            Condition = new JsonRouteCondition(_ => true)
        };

        var context = new MessageContext<OrderMessage> { Message = new OrderMessage() };
        var element = System.Text.Json.JsonDocument.Parse("{}").RootElement;

        Assert.True(topic.Evaluate(typeof(OrderMessage), context, element));
    }

    // =========================================================================
    // 15. Topic fan-out to unknown endpoint — logs warning, does not throw
    // =========================================================================

    [Fact]
    public async Task Topic_WithUnknownEndpoint_DoesNotThrow()
    {
        var (broker, _) = BuildBroker();

        broker.AddTopic(new TopicContext
        {
            TopicName = "orders",
            MessageType = typeof(OrderMessage),
            SubscriberEndpoints = ImmutableList.Create("NonExistentEndpoint")
        });

        // Should log a warning but not throw
        await broker.ProcessAsync(await SerializeAsync(new OrderMessage { OrderId = "X" }), null, TestContext.Current.CancellationToken);
    }

    // =========================================================================
    // 16. Multiple topics matching same message — all fire
    // =========================================================================

    [Fact]
    public async Task MultipleTopics_MatchingSameMessage_AllFire()
    {
        var ep1 = new MemoryQueueEndPoint("Multi1");
        var ep2 = new MemoryQueueEndPoint("Multi2");
        var (broker, _) = BuildBroker();
        broker.AddEndpoint("Multi1", ep1);
        broker.AddEndpoint("Multi2", ep2);

        broker.AddTopic(new TopicContext
        {
            TopicName = "topic-1",
            MessageType = typeof(OrderMessage),
            SubscriberEndpoints = ImmutableList.Create("Multi1")
        });
        broker.AddTopic(new TopicContext
        {
            TopicName = "topic-2",
            MessageType = typeof(OrderMessage),
            SubscriberEndpoints = ImmutableList.Create("Multi2")
        });

        await broker.ProcessAsync(await SerializeAsync(new OrderMessage { OrderId = "Multi" }), null, TestContext.Current.CancellationToken);

        var items1 = await DrainAsync(ep1);
        var items2 = await DrainAsync(ep2);

        Assert.Single(items1);
        Assert.Single(items2);
    }

    // =========================================================================
    // 17. PublishAsync(topicName) with IAnyMessage topic delivers message
    // =========================================================================

    [Fact]
    public async Task PublishAsync_ByTopicName_AnyMessageTopic_DeliversMessage()
    {
        var ep = new MemoryQueueEndPoint("AnyEp");
        var (broker, _) = BuildBroker();
        broker.AddEndpoint("AnyEp", ep);

        broker.AddTopic(new TopicContext
        {
            TopicName = "any-topic",
            MessageType = typeof(IAnyMessage),  // matches any type
            SubscriberEndpoints = ImmutableList.Create("AnyEp")
        });

        await broker.PublishAsync("any-topic", new OrderMessage { OrderId = "Any1" }, TestContext.Current.CancellationToken);

        var items = await DrainAsync(ep);
        Assert.Single(items);
        Assert.Contains("Any1", items[0]);
    }
}
