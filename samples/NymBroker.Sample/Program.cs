using NymBroker.Core.DI;
using NymBroker.Core.Impl;
using NymBroker.Sample.Consumers;
using NymBroker.Sample.Messages;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((_, services) =>
    {
        services.AddNymBroker()
            .AddMemoryEndPoint("MemQueue")
            .AddFileEndPoint("FileOut")
            .AddConsumer<TradingConsumer>()   // handles OrderMessage + StockPriceMessage
            .Build();
    })
    .Build();

var broker = host.Services.GetRequiredService<INymBroker>();

// Route high-priority orders to FileOut in addition to consuming them.
broker.Route<OrderMessage>()
    .To("FileOut")
    .When(msg => msg.TryGetProperty("priority", out var p) && p.GetString() == "high")
    .Build();

// --- Interval action: post a stock price every 10 seconds ---
// Useful for polling external feeds, keep-alives, or periodic batch triggers.
broker.AddScheduledAction<INymBroker>(
    TimeSpan.FromSeconds(10),
    b => b.PostAsync("MemQueue", new StockPriceMessage
    {
        Ticker = "ACME",
        Price = Math.Round(100m + (decimal)Random.Shared.NextSingle() * 20, 2),
        AsOf = DateTime.UtcNow
    }).GetAwaiter().GetResult(),
    broker);

// --- Cron action: post a daily close price summary at the start of every minute ---
// Expression "* * * * *" = every minute (use "0 17 * * 1-5" for Mon–Fri at 17:00).
// Action<T1> is synchronous, so async calls inside need .GetAwaiter().GetResult().
broker.AddScheduledAction<INymBroker>(
    "* * * * *",
    b => b.PostAsync("MemQueue", new StockPriceMessage
    {
        Ticker = "CRON-DEMO",
        Price = 42.00m,
        AsOf = DateTime.UtcNow
    }).GetAwaiter().GetResult(),
    broker);

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

await host.StartAsync(cts.Token);

Console.WriteLine("Broker running. Scheduled actions:");
Console.WriteLine("  Interval — StockPriceMessage every 10 s  (fires immediately after jitter)");
Console.WriteLine("  Cron     — StockPriceMessage every minute (\"* * * * *\")");
Console.WriteLine("  Orders   — posted once below, high-priority also routed to FileOut");
Console.WriteLine("Press Ctrl+C to stop.");
Console.WriteLine();

// One-shot order messages to demonstrate the order pipeline.
await broker.PostAsync("MemQueue", new OrderMessage
{
    OrderId = "ORD-001",
    Customer = "Alice",
    Amount = 299.99m,
    Priority = "high"
});

await broker.PostAsync("MemQueue", new OrderMessage
{
    OrderId = "ORD-002",
    Customer = "Bob",
    Amount = 49.00m,
    Priority = "normal"
});

try { await Task.Delay(Timeout.Infinite, cts.Token); }
catch (OperationCanceledException) { }

await host.StopAsync();
Console.WriteLine("Broker stopped.");
