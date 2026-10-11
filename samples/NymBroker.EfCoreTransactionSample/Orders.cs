using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Consume;
using NymBroker.Core.Message;

namespace NymBroker.EfCoreTransactionSample;

[MessageName("orders.place-order")]
public sealed record PlaceOrder(string OrderId, string Customer, IReadOnlyList<OrderLineItem> Lines);

public sealed record OrderLineItem(string ProductId, int Quantity);

public sealed class Order
{
    public required string OrderId { get; init; }
    public required string Customer { get; init; }
    public DateTime CreatedAtUtc { get; init; }
    public List<OrderLine> Lines { get; init; } = [];
}

public sealed class OrderLine
{
    public int Id { get; init; }
    public required string OrderId { get; init; }
    public required string ProductId { get; init; }
    public int Quantity { get; init; }
}

public sealed class Product
{
    public required string ProductId { get; init; }
    public required string Name { get; init; }
    public int Stock { get; set; }
}

public sealed class OrdersDbContext(DbContextOptions<OrdersDbContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OrderLine> OrderLines => Set<OrderLine>();
    public DbSet<Product> Products => Set<Product>();

    protected override void OnModelCreating(ModelBuilder model)
    {
        model.Entity<Order>(e =>
        {
            e.HasKey(o => o.OrderId);
            e.HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId);
        });
        model.Entity<OrderLine>().HasOne<Product>().WithMany().HasForeignKey(l => l.ProductId);
        model.Entity<Product>().HasKey(p => p.ProductId);
    }
}

/// <summary>
/// Places an order: writes the order, its lines and the stock of every product in ONE database transaction, so
/// either all three tables change or none does. Everything here shares the message's DI scope, so the DbContext
/// (scoped) — and anything else scoped this consumer depends on — uses the same connection and transaction, and is
/// disposed when the message is done. Duplicates never reach this class: the broker's SQL Server idempotency store
/// (AddSqlServerIdempotency in Program.cs) drops a message whose id was already handled.
/// </summary>
public sealed class PlaceOrderConsumer(OrdersDbContext db, ILogger<PlaceOrderConsumer> logger) : IConsume<PlaceOrder>
{
    public async Task ConsumeAsync(PlaceOrder message, IMessageContext context, CancellationToken ct = default)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        // Tables 1 and 2: the order and its lines. Sent to the database now, but not durable until Commit.
        db.Orders.Add(new Order
        {
            OrderId = message.OrderId,
            Customer = message.Customer,
            CreatedAtUtc = DateTime.UtcNow,
            Lines = [.. message.Lines.Select(l => new OrderLine { OrderId = message.OrderId, ProductId = l.ProductId, Quantity = l.Quantity })]
        });
        await db.SaveChangesAsync(ct);

        // Table 3: reserve stock, one line at a time. The UPDATE only succeeds while enough stock is left, so two
        // orders racing for the last items cannot both get them. It runs inside the same transaction.
        foreach (var line in message.Lines)
        {
            var reserved = await db.Products
                .Where(p => p.ProductId == line.ProductId && p.Stock >= line.Quantity)
                .ExecuteUpdateAsync(p => p.SetProperty(x => x.Stock, x => x.Stock - line.Quantity), ct);

            // Out of stock: throw. Disposing the uncommitted transaction rolls back the order, its lines and the stock
            // already reserved for earlier lines. The broker returns Retry, which also releases the message's
            // idempotency claim, and the SQL Server queue redelivers it (until MaxRetryCount, then marks it Failed).
            if (reserved == 0)
                throw new InvalidOperationException($"Order {message.OrderId}: not enough stock of {line.ProductId} for {line.Quantity}.");
        }

        await tx.CommitAsync(ct);   // all three tables change together
        logger.LogInformation("Order {OrderId} placed with {Lines} line(s) (message {MessageId})", message.OrderId, message.Lines.Count, context.Id);
    }
}
