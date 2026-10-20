using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Factory;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Route;
using NymBroker.Core.Serialize;

namespace NymBroker.Tests;

/// <summary>Chained conditions on routes and topics are AND-ed together (#47).</summary>
public sealed class RouteConditionChainingTests
{
    [MessageName("tests.chaining.order")]
    public sealed record ChainOrder(decimal Amount = 0m, string Priority = "normal");

    private sealed class RecordingEndPoint : IEndPoint
    {
        public ConcurrentQueue<byte[]> Posted { get; } = new();
        public Task PostAsync(byte[] message, CancellationToken ct = default) { Posted.Enqueue(message); return Task.CompletedTask; }
        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
    }

    private sealed class ConditionedRouteContext : RouteContext
    {
        public ConditionedRouteContext() => Condition = new JsonRouteCondition(m => m.GetProperty("amount").GetDecimal() > 1000m);
    }

    private static (INymBroker Broker, RecordingEndPoint Dest, MessageSerializerJson Serializer) Build(Action<NymBrokerBuilder>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var dest = new RecordingEndPoint();
        var builder = services.AddNymBroker();
        builder.Services.AddKeyedSingleton<IEndPoint>("Dest", dest);
        builder.RegisterEndpoint("Dest");
        configure?.Invoke(builder);
        builder.Build();
        var sp = services.BuildServiceProvider();
        return (sp.GetRequiredService<INymBroker>(), dest, sp.GetRequiredService<MessageSerializerJson>());
    }

    private static async Task SendAsync(INymBroker broker, MessageSerializerJson serializer, ChainOrder order, DateTime? created = null)
    {
        var context = new MessageContext<ChainOrder> { Message = order };
        if (created.HasValue) context.Created = created.Value;
        using var stream = serializer.Serialize(context);
        using var copy = new MemoryStream();
        await stream.CopyToAsync(copy, TestContext.Current.CancellationToken);
        await broker.ProcessAsync(copy.ToArray(), "Source", TestContext.Current.CancellationToken);
    }

    private static List<ChainOrder> Delivered(RecordingEndPoint dest, MessageSerializerJson serializer)
        => dest.Posted.Select(b => MessageSerializerJson.DeserializeMessage<ChainOrder>((RawMessageContext)serializer.Deserialize(b.AsSpan()))!).ToList();

    private static bool IsHigh(System.Text.Json.JsonElement m) => m.GetProperty("priority").GetString() == "high";
    private static bool IsLarge(System.Text.Json.JsonElement m) => m.GetProperty("amount").GetDecimal() > 1000m;

    [Fact]
    public async Task Route_ChainedWhen_RequiresAllConditions()
    {
        var (broker, dest, serializer) = Build(b => b.AddRoute<ChainOrder>(r => r.To("Dest").When(IsLarge).When(IsHigh)));

        await SendAsync(broker, serializer, new ChainOrder(1500m, "high"));
        await SendAsync(broker, serializer, new ChainOrder(1500m, "low"));    // before #47: routed (only the last When was checked)
        await SendAsync(broker, serializer, new ChainOrder(10m, "high"));

        Assert.Equal([new ChainOrder(1500m, "high")], Delivered(dest, serializer));
    }

    [Fact]
    public async Task Route_WhenThenOlderThan_RequiresBoth()
    {
        var (broker, dest, serializer) = Build(b => b.AddRoute<ChainOrder>(r => r.To("Dest").When(IsHigh).WhenMessageIsOlderThan(TimeSpan.FromHours(1))));
        var old = DateTime.UtcNow.AddHours(-2);

        await SendAsync(broker, serializer, new ChainOrder(1m, "high"), old);
        await SendAsync(broker, serializer, new ChainOrder(2m, "high"));        // new
        await SendAsync(broker, serializer, new ChainOrder(3m, "low"), old);    // not high

        Assert.Equal([new ChainOrder(1m, "high")], Delivered(dest, serializer));
    }

    [Fact]
    public async Task Route_OrThenWhen_IsOrAndedWithWhen()
    {
        var (broker, dest, serializer) = Build(b => b.AddRoute<ChainOrder>(r => r
            .To("Dest")
            .Or(new JsonRouteCondition(IsLarge), new JsonRouteCondition(IsHigh))   // large OR high ...
            .When(m => m.GetProperty("amount").GetDecimal() < 5000m)));            // ... AND below 5000

        await SendAsync(broker, serializer, new ChainOrder(1500m, "low"));   // large, < 5000
        await SendAsync(broker, serializer, new ChainOrder(10m, "high"));    // high, < 5000
        await SendAsync(broker, serializer, new ChainOrder(9000m, "high"));  // fails the When
        await SendAsync(broker, serializer, new ChainOrder(10m, "low"));     // fails the Or

        Assert.Equal([new ChainOrder(1500m, "low"), new ChainOrder(10m, "high")], Delivered(dest, serializer));
    }

    [Fact]
    public async Task Route_AndThenWhen_RequiresAll()
    {
        var (broker, dest, serializer) = Build(b => b.AddRoute<ChainOrder>(r => r
            .To("Dest")
            .And(new JsonRouteCondition(IsLarge), new JsonRouteCondition(IsHigh))
            .When(m => m.GetProperty("amount").GetDecimal() < 5000m)));

        await SendAsync(broker, serializer, new ChainOrder(1500m, "high"));
        await SendAsync(broker, serializer, new ChainOrder(9000m, "high"));

        Assert.Equal([new ChainOrder(1500m, "high")], Delivered(dest, serializer));
    }

    [Fact]
    public async Task Route_FactoryCondition_IsCombinedWithBuilderConditions()
    {
        var (broker, dest, serializer) = Build(b => b.AddRoute(
            () => new ConditionedRouteContext { MessageType = typeof(ChainOrder) },   // amount > 1000
            r => r.To("Dest").When(IsHigh)));

        await SendAsync(broker, serializer, new ChainOrder(1500m, "high"));
        await SendAsync(broker, serializer, new ChainOrder(1500m, "low"));
        await SendAsync(broker, serializer, new ChainOrder(10m, "high"));

        Assert.Equal([new ChainOrder(1500m, "high")], Delivered(dest, serializer));
    }

    [Fact]
    public async Task Topic_ChainedWhen_RequiresAllConditions()
    {
        var (broker, dest, serializer) = Build(b => b
            .AddTopic<ChainOrder>("orders")
            .When(IsLarge)
            .When(new JsonRouteCondition(IsHigh))
            .SubscribeTo("Dest")
            .Build());

        await SendAsync(broker, serializer, new ChainOrder(1500m, "high"));
        await SendAsync(broker, serializer, new ChainOrder(1500m, "low"));
        await SendAsync(broker, serializer, new ChainOrder(10m, "high"));

        Assert.Equal([new ChainOrder(1500m, "high")], Delivered(dest, serializer));
    }

    [Fact]
    public async Task Route_SingleCondition_StillWorks()
    {
        var (broker, dest, serializer) = Build(b => b.AddRoute<ChainOrder>(r => r.To("Dest").When(IsHigh)));

        await SendAsync(broker, serializer, new ChainOrder(1m, "high"));
        await SendAsync(broker, serializer, new ChainOrder(2m, "low"));

        Assert.Equal([new ChainOrder(1m, "high")], Delivered(dest, serializer));
    }

    // --- Builder-only configuration: AddRoute / AddFilter ---

    [Fact]
    public void Builder_AddRoute_WithoutTo_ThrowsWhenAdded()
    {
        var builder = new ServiceCollection().AddNymBroker();
        Assert.Throws<InvalidOperationException>(() => builder.AddRoute<ChainOrder>(r => r.When(IsHigh)));
    }

    [Fact]
    public async Task Builder_AddRoute_ToUnknownEndpoint_StartAsyncThrows()
    {
        var (broker, _, _) = Build(b => b.AddRoute<ChainOrder>(r => r.To("Missing")));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => broker.StartAsync(TestContext.Current.CancellationToken));
        Assert.Contains("'Missing' is not a registered endpoint", ex.Message);
    }

    [Fact]
    public async Task Builder_AddRoute_AnyMessageType_RoutesEveryType()
    {
        var (broker, dest, serializer) = Build(b => b.AddRoute(r => r.To("Dest")));

        await SendAsync(broker, serializer, new ChainOrder(1m, "low"));

        Assert.Single(Delivered(dest, serializer));
    }

    private sealed class DropLowPriority : Core.Filter.IMessageFilter
    {
        public IMessageContext? Filter(IMessageContext context)
            => context is RawMessageContext raw && raw.RawMessage.GetProperty("priority").GetString() == "low" ? null : context;
    }

    private sealed class DropSmall : Core.Filter.IMessageFilter
    {
        public IMessageContext? Filter(IMessageContext context)
            => context is RawMessageContext raw && raw.RawMessage.GetProperty("amount").GetDecimal() < 10m ? null : context;
    }

    [Fact]
    public async Task Builder_AddFilter_InstanceAndDiFilter_BothRun()
    {
        var (broker, dest, serializer) = Build(b => b
            .AddFilter(new DropLowPriority())
            .AddFilter<DropSmall>()
            .AddRoute<ChainOrder>(r => r.To("Dest")));

        await SendAsync(broker, serializer, new ChainOrder(50m, "high"));
        await SendAsync(broker, serializer, new ChainOrder(50m, "low"));    // dropped by the instance filter
        await SendAsync(broker, serializer, new ChainOrder(1m, "high"));    // dropped by the DI filter

        Assert.Equal([new ChainOrder(50m, "high")], Delivered(dest, serializer));
    }
}
