using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NymBroker.Core.DI;
using NymBroker.Core.Impl;
using NymBroker.CsvSample.Consumers;
using NymBroker.CsvSample.Transform;

// Usage:
//   dotnet run                         # posts the built-in CSV orders
//   dotnet run -- "ORD-99,Zara,12.50,low"   # posts a single custom CSV line

Console.OutputEncoding = Encoding.UTF8;
{
    var w = new StreamWriter(Console.OpenStandardOutput(), Encoding.UTF8, leaveOpen: true) { AutoFlush = true };
    Console.SetOut(w);
}

const string Endpoint = "CsvInbox";

var csvLines = args.Length > 0
    ? args
    : new[]
    {
        "ORD-001,Alice,499.99,high",
        "ORD-002,Bob,89.50,normal",
        "ORD-003,Carol,1299.00,high",
        "bad-line-should-be-dropped",
        "ORD-004,Dave,24.99,low",
    };

var host = Host.CreateDefaultBuilder()
    .ConfigureLogging(l => l.SetMinimumLevel(LogLevel.Information))
    .ConfigureServices((_, services) =>
    {
        services.AddNymBroker()
            .AddMemoryEndPoint(Endpoint)
            .AddInputTransformer<CsvOrderTransformer>(Endpoint)
            .AddConsumer<OrderConsumer>()
            .Build();
    })
    .Build();

await host.StartAsync();

var broker = host.Services.GetRequiredService<INymBroker>();

Console.WriteLine($"CsvSample  endpoint={Endpoint}");
Console.WriteLine($"Posting {csvLines.Length} line(s)...");
Console.WriteLine();

foreach (var line in csvLines)
{
    var bytes = Encoding.UTF8.GetBytes(line);
    await broker.PostAsync(Endpoint, (Stream)new MemoryStream(bytes));
    Console.WriteLine($"  posted: {line}");
}

Console.WriteLine();
// Give the in-process listener a moment to drain the queue.
await Task.Delay(300);

await host.StopAsync();
Console.WriteLine("Done.");
