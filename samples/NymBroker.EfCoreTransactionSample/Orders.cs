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

/// <summary>An order that could not be placed, and why. Written in the order's transaction instead of the order.</summary>
public sealed class OrderRejection
{
    public required string OrderId { get; init; }
    public required string Reason { get; init; }
    public DateTime RejectedAtUtc { get; init; }
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
    public DbSet<OrderRejection> OrderRejections => Set<OrderRejection>();

    protected override void OnModelCreating(ModelBuilder model)
    {
        model.Entity<Order>(e =>
        {
            e.HasKey(o => o.OrderId);
            e.HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId);
        });
        model.Entity<OrderLine>().HasOne<Product>().WithMany().HasForeignKey(l => l.ProductId);
        model.Entity<Product>().HasKey(p => p.ProductId);
        model.Entity<OrderRejection>().HasKey(r => r.OrderId);
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
    private const string BeforeOrder = "BeforeOrder";

    public async Task ConsumeAsync(PlaceOrder message, IMessageContext context, CancellationToken ct = default)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        await tx.CreateSavepointAsync(BeforeOrder, ct);

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

            // Out of stock is a business outcome, not a failure: retrying would only fail again, and dead-lettering is
            // for messages that could not be handled at all. Undo the order, its lines and the stock already reserved
            // for earlier lines (back to the savepoint), record why instead, and commit — so the rejection is written
            // if and only if the order is not. Returning normally completes the message: no retry, no dead letter.
            if (reserved == 0)
            {
                await RejectAsync(tx, message, $"not enough stock of {line.ProductId} for {line.Quantity}", ct);
                return;
            }
        }

        // Anything unexpected (deadlock, lost connection) still throws before this line: disposing the uncommitted
        // transaction rolls everything back, the broker returns Retry and the queue redelivers the message.
        await tx.CommitAsync(ct);   // all three tables change together
        logger.LogInformation("Order {OrderId} placed with {Lines} line(s) (message {MessageId})", message.OrderId, message.Lines.Count, context.Id);
    }

    private async Task RejectAsync(Microsoft.EntityFrameworkCore.Storage.IDbContextTransaction tx, PlaceOrder message, string reason, CancellationToken ct)
    {
        await tx.RollbackToSavepointAsync(BeforeOrder, ct);
        db.ChangeTracker.Clear();   // the order entity added above was never kept; don't let EF Core track it
        db.OrderRejections.Add(new OrderRejection { OrderId = message.OrderId, Reason = reason, RejectedAtUtc = DateTime.UtcNow });
        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
        logger.LogWarning("Order {OrderId} rejected: {Reason}", message.OrderId, reason);
    }
}
