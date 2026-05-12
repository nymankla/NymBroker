using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NymBroker.Core.DI;
using NymBroker.Core.Impl;
using NymBroker.RabbitMq;
using NymBroker.RabbitSample.Consumers;
using NymBroker.RabbitSample.Messages;

// Usage:
//   dotnet run                        # posts 5 orders, listens until Ctrl+C
//   dotnet run -- --count 20          # posts 20 orders
//   dotnet run -- --mode producer     # posts only (no consumer)
//   dotnet run -- --mode consumer     # listens only (no posting)

// Flush on every write so output appears immediately when stdout is redirected.
Console.OutputEncoding = System.Text.Encoding.UTF8;
{
    var w = new StreamWriter(Console.OpenStandardOutput(), System.Text.Encoding.UTF8, leaveOpen: true) { AutoFlush = true };
    Console.SetOut(w);
}

const string Queue = "nymbroker.orders";

var countArg = args.SkipWhile(a => a != "--count").Skip(1).FirstOrDefault();
var count    = int.TryParse(countArg, out var n) && n > 0 ? n : 5;
var mode     = args.SkipWhile(a => a != "--mode").Skip(1).FirstOrDefault()?.ToLowerInvariant() ?? "both";

var rabbitSettings = new RabbitMqSettings
{
    HostName       = "localhost",
    ReadQueueName  = mode != "producer" ? Queue : string.Empty,
    WriteQueueName = mode != "consumer" ? Queue : string.Empty,
};

var endpointMode = mode switch
{
    "producer" => NymBroker.Core.Endpoint.EndpointMode.WriteOnly,
    "consumer" => NymBroker.Core.Endpoint.EndpointMode.ReadWrite,
    _          => NymBroker.Core.Endpoint.EndpointMode.ReadWrite,
};

var host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(l => l.SetMinimumLevel(LogLevel.Warning))
    .ConfigureServices((_, services) =>
    {
        var builder = services.AddNymBroker()
            .AddRabbitMqEndPoint("Orders", rabbitSettings, endpointMode);

        if (mode != "producer")
            builder.AddConsumer<OrderConsumer>();

        builder.Build();
    })
    .Build();

var broker = host.Services.GetRequiredService<INymBroker>();

Console.WriteLine($"RabbitSample  queue={Queue}  mode={mode}");
Console.WriteLine($"  RabbitMQ: amqp://guest:guest@localhost:5672");
Console.WriteLine($"  Management: http://localhost:15672");
Console.WriteLine();

if (mode != "consumer")
{
    var customers  = new[] { "Alice", "Bob", "Carol", "Dave", "Eve" };
    var priorities = new[] { "high", "normal", "low" };
    var rng        = new Random(42);

    Console.WriteLine($"Posting {count} order(s) to '{Queue}'...");
    for (var i = 1; i <= count; i++)
    {
        var msg = new OrderMessage(
            $"ORD-{i:D4}",
            customers[rng.Next(customers.Length)],
            Math.Round((decimal)(rng.NextDouble() * 1999 + 1), 2),
            priorities[rng.Next(priorities.Length)]);
        await broker.PostAsync("Orders", msg);
    }
    Console.WriteLine($"{count} message(s) posted.");
    Console.WriteLine();
}

if (mode == "producer")
{
    await host.StopAsync();
    return;
}

Console.WriteLine("Listening for orders — Ctrl+C to stop.");
Console.WriteLine();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

await host.StartAsync(cts.Token);

try { await Task.Delay(Timeout.Infinite, cts.Token); }
catch (OperationCanceledException) { }

await host.StopAsync();
Console.WriteLine("Stopped.");
