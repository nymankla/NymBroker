using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;

namespace NymBroker.Tests;

/// <summary>A message type has exactly one consumer; conflicts fail loudly instead of silently replacing (#49).</summary>
public sealed class DuplicateConsumerTests
{
    [MessageName("tests.duplicate.order")]
    public sealed record DupOrder(int Id = 0);

    [MessageName("tests.duplicate.invoice")]
    public sealed record DupInvoice(int Id = 0);

    public sealed class BillingConsumer : IConsume<DupOrder>
    {
        public Task ConsumeAsync(DupOrder message, IMessageContext context, CancellationToken ct = default) => Task.CompletedTask;
    }

    public sealed class ShippingConsumer : IConsume<DupOrder>
    {
        public Task ConsumeAsync(DupOrder message, IMessageContext context, CancellationToken ct = default) => Task.CompletedTask;
    }

    public sealed class InvoiceConsumer : IConsume<DupInvoice>
    {
        public Task ConsumeAsync(DupInvoice message, IMessageContext context, CancellationToken ct = default) => Task.CompletedTask;
    }

    public sealed class OrderAndInvoiceConsumer : IConsume<DupOrder>, IConsume<DupInvoice>
    {
        public Task ConsumeAsync(DupOrder message, IMessageContext context, CancellationToken ct = default) => Task.CompletedTask;
        public Task ConsumeAsync(DupInvoice message, IMessageContext context, CancellationToken ct = default) => Task.CompletedTask;
    }

    // Two different classes with the same (simple) class name — consumers are keyed by class name.
    public static class First { public sealed class SameName : IConsume<DupOrder> { public Task ConsumeAsync(DupOrder m, IMessageContext c, CancellationToken ct = default) => Task.CompletedTask; } }
    public static class Second { public sealed class SameName : IConsume<DupInvoice> { public Task ConsumeAsync(DupInvoice m, IMessageContext c, CancellationToken ct = default) => Task.CompletedTask; } }

    [Fact]
    public void AddConsumer_SecondConsumerForSameType_Throws_NamingBothAndPointingToTopics()
    {
        var builder = new ServiceCollection().AddNymBroker().AddConsumer<BillingConsumer>();

        var ex = Assert.Throws<InvalidOperationException>(() => builder.AddConsumer<ShippingConsumer>());

        Assert.Contains("tests.duplicate.order", ex.Message);
        Assert.Contains(nameof(BillingConsumer), ex.Message);
        Assert.Contains(nameof(ShippingConsumer), ex.Message);
        Assert.Contains("ISubscribe<T>", ex.Message);
    }

    [Fact]
    public void AddConsumer_OverlappingMultiTypeConsumer_Throws_AndRegistersNothingForIt()
    {
        var services = new ServiceCollection();
        var builder = services.AddNymBroker().AddConsumer<InvoiceConsumer>();

        Assert.Throws<InvalidOperationException>(() => builder.AddConsumer<OrderAndInvoiceConsumer>());

        // Nothing half-registered: the failing consumer was not added to DI.
        Assert.DoesNotContain(services, d => d.IsKeyedService && Equals(d.ServiceKey, nameof(OrderAndInvoiceConsumer)));
    }

    [Fact]
    public async Task AddConsumer_SameConsumerTwice_IsHarmless_AndDispatchesOnce()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .AddMemoryEndPoint("Orders")
            .AddConsumer<BillingConsumer>()
            .AddConsumer<BillingConsumer>()
            .Build();

        Assert.Single(services, d => d.IsKeyedService && Equals(d.ServiceKey, nameof(BillingConsumer)));
        await using var sp = services.BuildServiceProvider();
        Assert.NotNull(sp.GetRequiredService<INymBroker>());   // building the broker registers it without error
    }

    [Fact]
    public void AddConsumer_DifferentClassesWithSameName_Throws()
    {
        var builder = new ServiceCollection().AddNymBroker().AddConsumer<First.SameName>();

        var ex = Assert.Throws<InvalidOperationException>(() => builder.AddConsumer<Second.SameName>());

        Assert.Contains("same class name", ex.Message);
    }

    [Fact]
    public void AddConsumer_DifferentTypes_AreFine()
    {
        new ServiceCollection().AddNymBroker()
            .AddConsumer<BillingConsumer>()
            .AddConsumer<InvoiceConsumer>()
            .Build();
    }

    [Fact]
    public void Dispatcher_RegisterConsumer_SameKeyAgain_IsNoOp_DifferentKeyThrows()
    {
        var services = new ServiceCollection().BuildServiceProvider();
        var dispatcher = new ConsumerDispatcher(services.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance);

        dispatcher.RegisterConsumer(typeof(DupOrder), nameof(BillingConsumer));
        dispatcher.RegisterConsumer(typeof(DupOrder), nameof(BillingConsumer));

        Assert.Throws<InvalidOperationException>(() => dispatcher.RegisterConsumer(typeof(DupOrder), nameof(ShippingConsumer)));
    }
}
