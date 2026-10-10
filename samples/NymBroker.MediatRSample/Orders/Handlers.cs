using NymBroker.Core.Consume;
using NymBroker.Core.Filter;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;

namespace NymBroker.MediatRSample.Orders;

/// <summary>
/// The command handler (MediatR: IRequestHandler&lt;CreateOrder&gt;). Runs when the broker takes the command off the
/// "Commands" endpoint, in its own DI scope. Throwing makes the transport retry it (SQLite) or sends it to the
/// dead-letter endpoint (Memory) — the HTTP caller already got 202 and never sees the exception.
/// </summary>
public sealed class CreateOrderHandler(OrderStore store, INymBroker broker, ILogger<CreateOrderHandler> logger) : IConsume<CreateOrder>
{
    public async Task ConsumeAsync(CreateOrder command, IMessageContext context, CancellationToken ct = default)
    {
        var order = new Order(command.OrderId, command.Customer, command.Amount, DateTimeOffset.UtcNow);
        store.Save(order);
        logger.LogInformation("Order {OrderId} stored for {Customer} ({Amount})", order.Id, order.Customer, order.Amount);

        // Published even when a redelivery finds the order already saved: subscribers are idempotent, and skipping
        // it could lose the event if the first attempt failed after Save. (Exactly-once needs an outbox.)
        await broker.PublishAsync(new OrderCreated(order.Id, order.Customer, order.Amount, order.CreatedAt), ct);
    }
}

/// <summary>Notification handler 1 (MediatR: INotificationHandler&lt;OrderCreated&gt;): keeps the read model up to date.</summary>
public sealed class OrderSummaryProjection(OrderQueries queries) : ISubscribe<OrderCreated>
{
    public Task ReceiveAsync(OrderCreated e, IMessageContext context, CancellationToken ct = default)
    {
        queries.Upsert(new OrderSummary(e.OrderId, e.Customer, e.Amount, e.CreatedAt));
        return Task.CompletedTask;
    }
}

/// <summary>Notification handler 2: an independent side effect on the same event.</summary>
public sealed class SendConfirmationEmail(ILogger<SendConfirmationEmail> logger) : ISubscribe<OrderCreated>
{
    public Task ReceiveAsync(OrderCreated e, IMessageContext context, CancellationToken ct = default)
    {
        logger.LogInformation("Confirmation e-mail queued for {Customer}, order {OrderId}", e.Customer, e.OrderId);
        return Task.CompletedTask;
    }
}

/// <summary>
/// A pipeline step (MediatR: IPipelineBehavior). It runs before routing and dispatch for every received message;
/// unlike a behavior it cannot wrap the handler, and returning null drops the message silently instead of throwing
/// to the caller — so validation happens in the API before posting, not here.
/// </summary>
public sealed class AuditFilter(ILogger<AuditFilter> logger) : IMessageFilter
{
    public IMessageContext? Filter(IMessageContext context)
    {
        logger.LogInformation("Audit: {MessageType} {MessageId} from {Source}", context.MessageType, context.Id, context.Address?.From ?? "PublishAsync");
        return context;
    }
}
