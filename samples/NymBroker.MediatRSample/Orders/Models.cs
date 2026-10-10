using System.Collections.Concurrent;

namespace NymBroker.MediatRSample.Orders;

// shortcut: in-memory stores keep the sample self-contained; a real app uses a database for both models.

/// <summary>Write model: what the command side owns.</summary>
public sealed class OrderStore
{
    private readonly ConcurrentDictionary<Guid, Order> _orders = new();

    /// <summary>Upsert by id, so a redelivered command is harmless.</summary>
    public void Save(Order order) => _orders[order.Id] = order;
}

public sealed record Order(Guid Id, string Customer, decimal Amount, DateTimeOffset CreatedAt);

/// <summary>
/// Read model: shaped for the queries, updated from events (eventually consistent). Queries read it directly —
/// MediatR's IRequest&lt;TResponse&gt; has no NymBroker counterpart, and a read needs no queue.
/// </summary>
public sealed class OrderQueries
{
    private readonly ConcurrentDictionary<Guid, OrderSummary> _summaries = new();

    public OrderSummary? Get(Guid orderId) => _summaries.GetValueOrDefault(orderId);

    public IReadOnlyList<OrderSummary> All() => [.. _summaries.Values.OrderByDescending(s => s.CreatedAt)];

    internal void Upsert(OrderSummary summary) => _summaries[summary.OrderId] = summary;
}

public sealed record OrderSummary(Guid OrderId, string Customer, decimal Amount, DateTimeOffset CreatedAt);
