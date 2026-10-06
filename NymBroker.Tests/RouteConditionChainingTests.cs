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
        var (broker, dest, serializer) = Build();
        broker.Route<ChainOrder>().To("Dest").When(IsLarge).When(IsHigh).Build();

        await SendAsync(broker, serializer, new ChainOrder(1500m, "high"));
        await SendAsync(broker, serializer, new ChainOrder(1500m, "low"));    // before #47: routed (only the last When was checked)
        await SendAsync(broker, serializer, new ChainOrder(10m, "high"));

        Assert.Equal([new ChainOrder(1500m, "high")], Delivered(dest, serializer));
    }

    [Fact]
    public async Task Route_WhenThenOlderThan_RequiresBoth()
    {
        var (broker, dest, serializer) = Build();
        broker.Route<ChainOrder>().To("Dest").When(IsHigh).WhenMessageIsOlderThan(TimeSpan.FromHours(1)).Build();
        var old = DateTime.UtcNow.AddHours(-2);

        await SendAsync(broker, serializer, new ChainOrder(1m, "high"), old);
        await SendAsync(broker, serializer, new ChainOrder(2m, "high"));        // new
        await SendAsync(broker, serializer, new ChainOrder(3m, "low"), old);    // not high

        Assert.Equal([new ChainOrder(1m, "high")], Delivered(dest, serializer));
    }

    [Fact]
    public async Task Route_OrThenWhen_IsOrAndedWithWhen()
    {
        var (broker, dest, serializer) = Build();
        broker.Route<ChainOrder>()
            .To("Dest")
            .Or(new JsonRouteCondition(IsLarge), new JsonRouteCondition(IsHigh))   // large OR high ...
            .When(m => m.GetProperty("amount").GetDecimal() < 5000m)               // ... AND below 5000
            .Build();

        await SendAsync(broker, serializer, new ChainOrder(1500m, "low"));   // large, < 5000
        await SendAsync(broker, serializer, new ChainOrder(10m, "high"));    // high, < 5000
        await SendAsync(broker, serializer, new ChainOrder(9000m, "high"));  // fails the When
        await SendAsync(broker, serializer, new ChainOrder(10m, "low"));     // fails the Or

        Assert.Equal([new ChainOrder(1500m, "low"), new ChainOrder(10m, "high")], Delivered(dest, serializer));
    }

    [Fact]
    public async Task Route_AndThenWhen_RequiresAll()
    {
        var (broker, dest, serializer) = Build();
        broker.Route<ChainOrder>()
            .To("Dest")
            .And(new JsonRouteCondition(IsLarge), new JsonRouteCondition(IsHigh))
            .When(m => m.GetProperty("amount").GetDecimal() < 5000m)
            .Build();

        await SendAsync(broker, serializer, new ChainOrder(1500m, "high"));
        await SendAsync(broker, serializer, new ChainOrder(9000m, "high"));

        Assert.Equal([new ChainOrder(1500m, "high")], Delivered(dest, serializer));
    }

    [Fact]
    public async Task Route_FactoryCondition_IsCombinedWithBuilderConditions()
    {
        var (broker, dest, serializer) = Build();
        broker.Route(() => new ConditionedRouteContext { MessageType = typeof(ChainOrder) })   // amount > 1000
            .To("Dest")
            .When(IsHigh)
            .Build();

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

    // --- #48: Transform(fileName) was never implemented ---

    [Fact]
    public void RouteTransform_IsMarkedObsolete()
    {
        var builderMethod = typeof(IRouteBuilder<ChainOrder>).GetMethod(nameof(IRouteBuilder<ChainOrder>.Transform))!;
        var contextProperty = typeof(RouteContext).GetProperty("Transform")!;

        Assert.Contains("#48", Assert.Single(builderMethod.GetCustomAttributes(typeof(ObsoleteAttribute), false).Cast<ObsoleteAttribute>()).Message);
        Assert.Contains("#48", Assert.Single(contextProperty.GetCustomAttributes(typeof(ObsoleteAttribute), false).Cast<ObsoleteAttribute>()).Message);
    }

    [Fact]
    public async Task RouteTransform_IsANoOp_TheMessageIsForwardedUnchanged()
    {
        var (broker, dest, serializer) = Build();
#pragma warning disable CS0618 // the call under test is obsolete
        broker.Route<ChainOrder>().To("Dest").Transform("order-to-invoice.xslt").Build();
#pragma warning restore CS0618

        await SendAsync(broker, serializer, new ChainOrder(42m, "high"));

        Assert.Equal([new ChainOrder(42m, "high")], Delivered(dest, serializer));
    }

    [Fact]
    public async Task Route_SingleCondition_StillWorks()
    {
        var (broker, dest, serializer) = Build();
        broker.Route<ChainOrder>().To("Dest").When(IsHigh).Build();

        await SendAsync(broker, serializer, new ChainOrder(1m, "high"));
        await SendAsync(broker, serializer, new ChainOrder(2m, "low"));

        Assert.Equal([new ChainOrder(1m, "high")], Delivered(dest, serializer));
    }
}
