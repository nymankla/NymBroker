using NymBroker.Core.Aggregator;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Impl;
using NymBroker.Core.PubSub;
using NymBroker.Core.Message;
using NymBroker.Core.Route;
using NymBroker.Core.Serialize;
using NymBroker.Core.Endpoint.HealthCheck;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;

namespace NymBroker.Tests;

public sealed class RoutingTests
{
    private sealed class PriceMessage { public decimal Price { get; set; } }

    private sealed class CostMessage { public decimal Cost { get; set; } }

    [MessageName("price.updated")]
    private sealed class NamedPriceMessage { public decimal Price { get; set; } }

    private sealed class ScopedDependency
    {
        public Guid Id { get; } = Guid.NewGuid();
    }

    private sealed class PriceConsumer : IConsume<PriceMessage>
    {
        public List<PriceMessage> Received { get; } = [];
        public Task ConsumeAsync(PriceMessage message, IMessageContext context, CancellationToken ct = default)
        {
            Received.Add(message);
            return Task.CompletedTask;
        }
    }

    private sealed class MultiMessageConsumer : IConsume<PriceMessage>, IConsume<CostMessage>
    {
        public static List<string> ReceivedTypes { get; } = [];

        public Task ConsumeAsync(PriceMessage message, IMessageContext context, CancellationToken ct = default)
        {
            lock (ReceivedTypes)
                ReceivedTypes.Add(nameof(PriceMessage));

            return Task.CompletedTask;
        }

        public Task ConsumeAsync(CostMessage message, IMessageContext context, CancellationToken ct = default)
        {
            lock (ReceivedTypes)
                ReceivedTypes.Add(nameof(CostMessage));

            return Task.CompletedTask;
        }
    }

    private sealed class NamedPriceConsumer : IConsume<NamedPriceMessage>
    {
        public static int ReceivedCount { get; private set; }

        public static void Reset() => ReceivedCount = 0;

        public Task ConsumeAsync(NamedPriceMessage message, IMessageContext context, CancellationToken ct = default)
        {
            ReceivedCount++;
            return Task.CompletedTask;
        }
    }

    private sealed class CustomRouteBuilder : IRouteBuilder
    {
        public RouteContext Build()
            => new()
            {
                MessageType = typeof(PriceMessage),
                DestinationEndpoint = "Dest",
                SourceEndpoint = "CustomSource"
            };
    }

    private sealed class BusinessHoursRouteContext : RouteContext
    {
        public override bool Evaluate(Type messageType, IMessageContext context, JsonElement messageElement)
            => base.Evaluate(messageType, context, messageElement) && DateTime.UtcNow.Hour >= 0;
    }

    private sealed class DisposableEndPoint(string name) : IEndPoint, IAsyncDisposable
    {
        public string Name { get; } = name;
        public bool Disposed { get; private set; }

        public Task PostAsync(Stream message, CancellationToken ct = default) => Task.CompletedTask;

        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TrackingEventDrivenEndPoint(string name) : IEndPointEventDriven
    {
        public string Name { get; } = name;
        public int StartCalls { get; private set; }
        public int StopCalls { get; private set; }

        public Task PostAsync(Stream message, CancellationToken ct = default) => Task.CompletedTask;

        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();

        public Task StartListeningAsync(Func<byte[], CancellationToken, Task> handler, CancellationToken ct)
        {
            StartCalls++;
            return Task.CompletedTask;
        }

        public Task StopListeningAsync()
        {
            StopCalls++;
            return Task.CompletedTask;
        }
    }

    private sealed class ScopedPriceConsumer(ScopedDependency dependency) : IConsume<PriceMessage>
    {
        public static List<Guid> DependencyIds { get; } = [];

        public Task ConsumeAsync(PriceMessage message, IMessageContext context, CancellationToken ct = default)
        {
            lock (DependencyIds)
                DependencyIds.Add(dependency.Id);

            return Task.CompletedTask;
        }
    }

    private static (NymBrokerImpl Broker, MemoryQueueEndPoint Dest, PriceConsumer Consumer) BuildBroker()
    {
        var consumer = new PriceConsumer();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKeyedSingleton<IMessageConsumer>(nameof(PriceConsumer), consumer);
        services.AddSingleton<MessageSerializerJson>();
        services.AddSingleton<IAggregator, AggregatorImpl>();

        var sp = services.BuildServiceProvider();

        var dest = new MemoryQueueEndPoint("Dest");
        var broker = new NymBrokerImpl(
            sp.GetRequiredService<MessageSerializerJson>(),
            sp.GetRequiredService<IAggregator>(),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            NullLogger<NymBrokerImpl>.Instance);

        broker.AddEndpoint(dest);
        broker.RegisterConsumer(typeof(PriceMessage), nameof(PriceConsumer));
        return (broker, dest, consumer);
    }

    private static NymBrokerImpl BuildScopedBroker()
    {
        ScopedPriceConsumer.DependencyIds.Clear();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddScoped<ScopedDependency>();
        services.AddKeyedScoped<IMessageConsumer, ScopedPriceConsumer>(nameof(ScopedPriceConsumer));
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

        broker.RegisterConsumer(typeof(PriceMessage), nameof(ScopedPriceConsumer));
        return broker;
    }

    [Fact]
    public async Task Consumer_ReceivesMessage_WhenNoRoutes()
    {
        var (broker, _, consumer) = BuildBroker();
        await broker.PostAsync("Dest", new PriceMessage { Price = 42m });

        // MemQueue forwards to ProcessAsync via listener — use direct ProcessAsync here.
        var ctx = new MessageContext<PriceMessage> { Message = new PriceMessage { Price = 42m } };
        var serializer = new MessageSerializerJson();
        using var stream = serializer.Serialize(ctx);
        using var reader = new System.IO.StreamReader(stream);
        var json = await reader.ReadToEndAsync();

        await broker.ProcessAsync(json, "Dest");
        Assert.Single(consumer.Received);
        Assert.Equal(42m, consumer.Received[0].Price);
    }

    [Fact]
    public async Task Route_WithCondition_OnlyMatchingMessageRouted()
    {
        var (broker, dest, _) = BuildBroker();
        broker.Route<PriceMessage>()
            .To("Dest")
            .When(el => el.TryGetProperty("price", out var p) && p.GetDecimal() > 100)
            .Build();

        var serializer = new MessageSerializerJson();

        async Task Send(decimal price)
        {
            var ctx = new MessageContext<PriceMessage> { Message = new PriceMessage { Price = price } };
            using var s = serializer.Serialize(ctx);
            using var r = new System.IO.StreamReader(s);
            await broker.ProcessAsync(await r.ReadToEndAsync(), null);
        }

        await Send(50m);   // should NOT route
        await Send(150m);  // should route to Dest

        var items = new List<string>();
        await foreach (var i in dest.ReadAsync()) items.Add(i);
        Assert.Single(items); // only the 150 message
    }

    [Fact]
    public async Task Routed_Message_IsNotConsumed()
    {
        var (broker, _, consumer) = BuildBroker();
        broker.Route<PriceMessage>()
            .To("Dest")
            .Build();

        var serializer = new MessageSerializerJson();
        var ctx = new MessageContext<PriceMessage> { Message = new PriceMessage { Price = 42m } };
        using var stream = serializer.Serialize(ctx);
        using var reader = new StreamReader(stream);

        await broker.ProcessAsync(await reader.ReadToEndAsync(), null);

        Assert.Empty(consumer.Received);
    }

    [Fact]
    public async Task Consumer_IsResolvedFromNewScope_PerMessage()
    {
        var broker = BuildScopedBroker();
        var serializer = new MessageSerializerJson();

        async Task Send(decimal price)
        {
            var ctx = new MessageContext<PriceMessage> { Message = new PriceMessage { Price = price } };
            using var s = serializer.Serialize(ctx);
            using var r = new System.IO.StreamReader(s);
            await broker.ProcessAsync(await r.ReadToEndAsync(), null);
        }

        await Send(1m);
        await Send(2m);

        Assert.Equal(2, ScopedPriceConsumer.DependencyIds.Count);
        Assert.NotEqual(ScopedPriceConsumer.DependencyIds[0], ScopedPriceConsumer.DependencyIds[1]);
    }

    [Fact]
    public async Task EndPoint_RegisteredInDi_IsDisposedWithProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<MessageSerializerJson>();
        services.AddSingleton<IAggregator, AggregatorImpl>();

        DisposableEndPoint? endpoint = null;
        services.AddKeyedSingleton<IEndPoint>("Disposable", (_, _) => endpoint = new DisposableEndPoint("Disposable"));

        services.AddSingleton<NymBrokerImpl>(sp =>
        {
            var broker = new NymBrokerImpl(
                sp.GetRequiredService<MessageSerializerJson>(),
                sp.GetRequiredService<IAggregator>(),
                new MessageTypeRegistry(),
                new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
                new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
                NullLogger<NymBrokerImpl>.Instance);

            broker.AddEndpoint(sp.GetRequiredKeyedService<IEndPoint>("Disposable"));
            return broker;
        });

        await using (var sp = services.BuildServiceProvider())
        {
            _ = sp.GetRequiredService<NymBrokerImpl>();
            Assert.NotNull(endpoint);
            Assert.False(endpoint!.Disposed);
        }

        Assert.True(endpoint!.Disposed);
    }

    [Fact]
    public async Task AddNymBroker_RegistersHostedService_ThatStartsAndStopsBroker()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var trackingEndPoint = new TrackingEventDrivenEndPoint("Tracked");
        services.AddKeyedSingleton<IEndPoint>("Tracked", trackingEndPoint);

        services.AddNymBroker().Build();

        await using var sp = services.BuildServiceProvider();

        var broker = (NymBrokerImpl)sp.GetRequiredService<INymBroker>();
        broker.AddEndpoint(sp.GetRequiredKeyedService<IEndPoint>("Tracked"));

        var hostedServices = sp.GetServices<IHostedService>().ToList();
        var hostedService = Assert.Single(hostedServices);

        await hostedService.StartAsync(CancellationToken.None);
        await hostedService.StopAsync(CancellationToken.None);

        Assert.Equal(1, trackingEndPoint.StartCalls);
        Assert.Equal(1, trackingEndPoint.StopCalls);
    }

    [Fact]
    public async Task AddConsumer_RegistersAllConsumeInterfaces()
    {
        MultiMessageConsumer.ReceivedTypes.Clear();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .AddConsumer<MultiMessageConsumer>()
            .Build();

        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<INymBroker>();
        var serializer = new MessageSerializerJson();

        async Task Send<T>(T message) where T : class
        {
            var context = new MessageContext<T> { Message = message };
            using var stream = serializer.Serialize(context);
            using var reader = new StreamReader(stream);
            await broker.ProcessAsync(await reader.ReadToEndAsync(), null);
        }

        await Send(new PriceMessage { Price = 10m });
        await Send(new CostMessage { Cost = 20m });

        Assert.Equal(2, MultiMessageConsumer.ReceivedTypes.Count);
        Assert.Contains(nameof(PriceMessage), MultiMessageConsumer.ReceivedTypes);
        Assert.Contains(nameof(CostMessage), MultiMessageConsumer.ReceivedTypes);
    }

    [Fact]
    public async Task Consumer_ReceivesMessage_ResolvedByExplicitMessageName()
    {
        NamedPriceConsumer.Reset();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .AddConsumer<NamedPriceConsumer>()
            .Build();

        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<INymBroker>();
        var serializer = new MessageSerializerJson();
        var context = new MessageContext<NamedPriceMessage> { Message = new NamedPriceMessage { Price = 12m } };

        using var stream = serializer.Serialize(context);
        using var reader = new StreamReader(stream);
        var json = await reader.ReadToEndAsync();

        await broker.ProcessAsync(json, null);

        Assert.Equal(1, NamedPriceConsumer.ReceivedCount);
    }

    [Fact]
    public async Task Route_WithoutGenericType_MatchesAnyMessage()
    {
        var (broker, dest, _) = BuildBroker();
        broker.Route()
            .WhenFrom("AnySource")
            .To("Dest")
            .Build();

        var serializer = new MessageSerializerJson();
        var context = new MessageContext<CostMessage> { Message = new CostMessage { Cost = 15m } };
        using var stream = serializer.Serialize(context);
        using var reader = new StreamReader(stream);

        await broker.ProcessAsync(await reader.ReadToEndAsync(), "AnySource");

        var items = new List<string>();
        await foreach (var item in dest.ReadAsync()) items.Add(item);
        Assert.Single(items);
    }

    [Fact]
    public void Route_CustomRouteContextFactory_PreservesDerivedType()
    {
        var (broker, _, _) = BuildBroker();

        var route = broker.Route(() => new BusinessHoursRouteContext { MessageType = typeof(PriceMessage) })
            .WhenFrom("FactorySource")
            .To("Dest")
            .Build();

        Assert.IsType<BusinessHoursRouteContext>(route);
    }

    [Fact]
    public async Task Route_WhenNotFrom_ExcludesMatchingSource()
    {
        var (broker, dest, _) = BuildBroker();
        broker.Route<PriceMessage>()
            .WhenNotFrom("Blocked")
            .To("Dest")
            .Build();

        var serializer = new MessageSerializerJson();

        async Task Send(string source)
        {
            var context = new MessageContext<PriceMessage> { Message = new PriceMessage { Price = 11m } };
            using var stream = serializer.Serialize(context);
            using var reader = new StreamReader(stream);
            await broker.ProcessAsync(await reader.ReadToEndAsync(), source);
        }

        await Send("Blocked");
        await Send("Allowed");

        var items = new List<string>();
        await foreach (var item in dest.ReadAsync()) items.Add(item);
        Assert.Single(items);
    }

    [Fact]
    public async Task Route_WhenMessageIsOlderThan_MatchesOldMessages()
    {
        var (broker, dest, _) = BuildBroker();
        broker.Route<PriceMessage>()
            .WhenMessageIsOlderThan(TimeSpan.FromMinutes(5))
            .To("Dest")
            .Build();

        var serializer = new MessageSerializerJson();
        var context = new MessageContext<PriceMessage>
        {
            Message = new PriceMessage { Price = 1m },
            Created = DateTime.UtcNow.AddMinutes(-10)
        };

        using var stream = serializer.Serialize(context);
        using var reader = new StreamReader(stream);
        await broker.ProcessAsync(await reader.ReadToEndAsync(), null);

        var items = new List<string>();
        await foreach (var item in dest.ReadAsync()) items.Add(item);
        Assert.Single(items);
    }

    [Fact]
    public async Task Route_And_Or_Conditions_AreSupported()
    {
        var (broker, dest, _) = BuildBroker();
        broker.Route<PriceMessage>()
            .And(
                new FromRouteCondition("Alpha"),
                new JsonRouteCondition(el => el.TryGetProperty("price", out var p) && p.GetDecimal() > 100m))
            .To("Dest")
            .Build();

        broker.Route<PriceMessage>()
            .Or(
                new FromRouteCondition("Beta"),
                new JsonRouteCondition(el => el.TryGetProperty("price", out var p) && p.GetDecimal() > 200m))
            .To("Dest")
            .Build();

        var serializer = new MessageSerializerJson();

        async Task Send(decimal price, string source)
        {
            var context = new MessageContext<PriceMessage> { Message = new PriceMessage { Price = price } };
            using var stream = serializer.Serialize(context);
            using var reader = new StreamReader(stream);
            await broker.ProcessAsync(await reader.ReadToEndAsync(), source);
        }

        await Send(150m, "Alpha");
        await Send(250m, "Gamma");

        var items = new List<string>();
        await foreach (var item in dest.ReadAsync()) items.Add(item);
        Assert.Equal(2, items.Count);
    }

    [Fact]
    public void Route_CustomBuilder_IsSupported()
    {
        var (broker, _, _) = BuildBroker();

        var route = broker.Route(new CustomRouteBuilder());

        Assert.Equal(typeof(PriceMessage), route.MessageType);
        Assert.Equal("Dest", route.DestinationEndpoint);
        Assert.Equal("CustomSource", route.SourceEndpoint);
    }

    [Fact]
    public async Task Route_CustomRouteContextFactory_IsSupported()
    {
        var (broker, dest, _) = BuildBroker();
        broker.Route(() => new BusinessHoursRouteContext { MessageType = typeof(PriceMessage) })
            .WhenFrom("FactorySource")
            .To("Dest")
            .Build();

        var serializer = new MessageSerializerJson();
        var context = new MessageContext<PriceMessage> { Message = new PriceMessage { Price = 99m } };
        using var stream = serializer.Serialize(context);
        using var reader = new StreamReader(stream);

        await broker.ProcessAsync(await reader.ReadToEndAsync(), "FactorySource");

        var items = new List<string>();
        await foreach (var item in dest.ReadAsync()) items.Add(item);
        Assert.Single(items);
    }

    [Fact]
    public async Task AddScheduledAction_ExecutesRecurringAction()
    {
        var (broker, _, _) = BuildBroker();
        var count = 0;
        using var ready = new ManualResetEventSlim(false);

        broker.AddScheduledAction(TimeSpan.FromMilliseconds(50), () =>
        {
            if (Interlocked.Increment(ref count) >= 1)
                ready.Set();
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await broker.StartAsync(cts.Token);

        Assert.True(ready.Wait(TimeSpan.FromSeconds(3)));

        await broker.StopAsync();
        Assert.True(count >= 1);
    }

    [Fact]
    public async Task StopAsync_StopsScheduledActions()
    {
        var (broker, _, _) = BuildBroker();
        var count = 0;
        using var ready = new ManualResetEventSlim(false);

        broker.AddScheduledAction(TimeSpan.FromMilliseconds(50), () =>
        {
            if (Interlocked.Increment(ref count) >= 1)
                ready.Set();
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await broker.StartAsync(cts.Token);
        Assert.True(ready.Wait(TimeSpan.FromSeconds(3)));

        await broker.StopAsync();
        var beforeWait = count;
        await Task.Delay(200);

        Assert.Equal(beforeWait, count);
    }

    [Fact]
    public async Task StartAsync_IsIdempotent_ForScheduledActions()
    {
        var (broker, _, _) = BuildBroker();
        var count = 0;
        using var ready = new ManualResetEventSlim(false);

        broker.AddScheduledAction(TimeSpan.FromMilliseconds(50), () =>
        {
            if (Interlocked.Increment(ref count) >= 1)
                ready.Set();
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await broker.StartAsync(cts.Token);
        await broker.StartAsync(cts.Token);

        Assert.True(ready.Wait(TimeSpan.FromSeconds(3)));
        var beforeWait = count;
        await Task.Delay(150);
        var afterWait = count;

        await broker.StopAsync();

        Assert.InRange(afterWait - beforeWait, 1, 4);
    }

    [Fact]
    public async Task StopAsync_IsIdempotent()
    {
        var (broker, _, _) = BuildBroker();
        broker.AddScheduledAction(TimeSpan.FromMilliseconds(50), () => { });

        await broker.StartAsync();
        await broker.StopAsync();
        await broker.StopAsync();
    }

    [Fact]
    public void Builder_Build_CanOnlyBeCalledOnce()
    {
        var services = new ServiceCollection();
        var builder = services.AddNymBroker();

        builder.Build();

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Fact]
    public async Task AddScheduledAction_CronExpression_IsAccepted()
    {
        var (broker, _, _) = BuildBroker();
        var invoked = false;

        broker.AddScheduledAction("* * * * *", value => value.Invoked = true, new CronInvocationState());

        await broker.StartAsync();
        await broker.StopAsync();
    }

    private sealed class CronInvocationState
    {
        public bool Invoked { get; set; }
    }
}
