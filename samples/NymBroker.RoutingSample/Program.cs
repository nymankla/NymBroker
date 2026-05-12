using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Impl;
using NymBroker.Core.Route;
using NymBroker.RoutingSample.Consumers;
using NymBroker.RoutingSample.Messages;
using NymBroker.RoutingSample.Subscribers;
using NymBroker.Sql;

static SqliteSettings Db(string table) => new()
{
    ConnectionString = "Data Source=:memory:",
    TableName        = table,
    AutoCreateTable  = true,
    PollInterval     = TimeSpan.FromMilliseconds(50),
};

var host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(l => l.SetMinimumLevel(LogLevel.Warning))
    .ConfigureServices((_, services) =>
    {
        services.AddNymBroker()
            // --- Endpoints ---
            .AddSqliteEndPoint("Inbox",    Db("Inbox"))
            .AddSqliteEndPoint("VIP",      Db("VIP"))
            .AddSqliteEndPoint("Standard", Db("Standard"))
            .AddSqliteEndPoint("AuditLog", Db("AuditLog"), EndpointMode.WriteOnly)

            // --- Consumer (handles OrderMessage from both VIP and Standard) ---
            .AddConsumer<OrderConsumer>()

            // --- Pub/Sub topic: fires on PublishAsync<OrderMessage> ---
            .AddTopic<OrderMessage>("order.events")
                .When(new AndRouteCondition(
                    new AndRouteCondition(
                        new NotFromRouteCondition("Inbox"),
                        new NotFromRouteCondition("VIP")),
                    new NotFromRouteCondition("Standard")))
                .SubscribeWith<AuditSubscriber>()
                .SubscribeWith<FraudCheckSubscriber>()
                .SubscribeTo("AuditLog")
                .Build()

            .Build();
    })
    .Build();

var broker = host.Services.GetRequiredService<INymBroker>();

broker.Route<OrderMessage>()
    .To("VIP")
    .WhenFrom("Inbox")
    .When(msg =>
        (msg.TryGetProperty("amount",   out var a) && a.GetDecimal() > 500m) ||
        (msg.TryGetProperty("priority", out var p) && p.GetString() == "high"))
    .Build();

broker.Route<OrderMessage>()
    .To("Standard")
    .WhenFrom("Inbox")
    .When(msg =>
        !(msg.TryGetProperty("amount",   out var a) && a.GetDecimal() > 500m) &&
        !(msg.TryGetProperty("priority", out var p) && p.GetString() == "high"))
    .Build();

await host.StartAsync();

// ─── Phase 1: Routing ────────────────────────────────────────────────────────

Console.WriteLine("=== Routing ===");
Console.WriteLine("  Orders arrive on Inbox and are routed to VIP (amount > £500 or priority=high)");
Console.WriteLine("  or Standard. The consumer logs which queue delivered each message.");
Console.WriteLine();

var routingOrders = new[]
{
    new OrderMessage("ORD-001", "Alice",   1_200.00m, "high"),    // → VIP   (both rules)
    new OrderMessage("ORD-002", "Bob",       750.00m, "normal"),  // → VIP   (amount > 500)
    new OrderMessage("ORD-003", "Carol",      49.99m, "normal"),  // → Standard
    new OrderMessage("ORD-004", "Dave",      200.00m, "high"),    // → VIP   (priority=high)
    new OrderMessage("ORD-005", "Eve",        15.00m, "normal"),  // → Standard
};

foreach (var order in routingOrders)
    await broker.PostAsync("Inbox", order);

await Task.Delay(500);  // two SQLite poll hops: Inbox→VIP/Standard→consumer

Console.WriteLine();

// ─── Phase 2: Pub/Sub ────────────────────────────────────────────────────────

Console.WriteLine("=== Pub/Sub ===");
Console.WriteLine("  PublishAsync bypasses endpoint routing and fires the 'order.events' topic.");
Console.WriteLine("  AuditSubscriber receives every order; FraudCheckSubscriber flags amount > £800.");
Console.WriteLine("  The AuditLog endpoint also receives a copy via endpoint fan-out.");
Console.WriteLine();

var pubSubOrders = new[]
{
    new OrderMessage("PUB-001", "Frank",   350.00m, "normal"),  // audit only
    new OrderMessage("PUB-002", "Grace", 2_500.00m, "high"),    // audit + fraud flag
    new OrderMessage("PUB-003", "Hank",    900.00m, "normal"),  // audit + fraud flag
};

foreach (var order in pubSubOrders)
    await broker.PublishAsync(order);

// Topic subscribers are dispatched inline by PublishAsync — no poll delay needed.

var auditLog = host.Services.GetRequiredKeyedService<IEndPoint>("AuditLog") as SqliteEndPoint;

var auditCount = 0;
if (auditLog != null)
    await foreach (var _ in auditLog.ReadAsync())
        auditCount++;

Console.WriteLine($"  [AuditLog] {auditCount} message(s) persisted to audit endpoint.");

await host.StopAsync();
