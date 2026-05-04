using MessageBroker.ConfigSample.Consumers;
using MessageBroker.ConfigSample.Messages;
using MessageBroker.Core.DI;
using MessageBroker.Core.Impl;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// Endpoints are wired entirely from queuesettings.json.
// Consumers and routes are still registered in code — config only covers transport topology.
var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((_, services) =>
    {
        services.AddMessageBroker()
            .LoadConfiguration("queuesettings.json")   // registers MemQueue + FileOut
            .AddConsumer<OrderConsumer>()
            .Build();
    })
    .Build();

var broker = host.Services.GetRequiredService<IMessageBroker>();

// High-priority orders are also written to FileOut (in addition to consumer dispatch).
broker.Route<OrderMessage>()
    .To("FileOut")
    .When(msg => msg.TryGetProperty("priority", out var p) && p.GetString() == "high")
    .Build();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

await host.StartAsync(cts.Token);

Console.WriteLine("Config-driven broker running.");
Console.WriteLine("  Endpoints loaded from: queuesettings.json");
Console.WriteLine("  MemQueue  — in-process memory queue");
Console.WriteLine("  FileOut   — writes to ./out/");
Console.WriteLine("  Route     — high-priority orders copied to FileOut");
Console.WriteLine("Press Ctrl+C to stop.");
Console.WriteLine();

// Post a few orders to MemQueue to demonstrate the pipeline.
await broker.PostAsync("MemQueue", new OrderMessage
{
    OrderId = "ORD-A01",
    Customer = "Alice",
    Amount = 499.00m,
    Priority = "high"          // routed to FileOut AND consumed
});

await broker.PostAsync("MemQueue", new OrderMessage
{
    OrderId = "ORD-A02",
    Customer = "Bob",
    Amount = 29.99m,
    Priority = "normal"        // consumed only
});

await broker.PostAsync("MemQueue", new OrderMessage
{
    OrderId = "ORD-A03",
    Customer = "Carol",
    Amount = 1200.00m,
    Priority = "high"          // routed to FileOut AND consumed
});

try { await Task.Delay(Timeout.Infinite, cts.Token); }
catch (OperationCanceledException) { }

await host.StopAsync();
Console.WriteLine("Broker stopped.");
