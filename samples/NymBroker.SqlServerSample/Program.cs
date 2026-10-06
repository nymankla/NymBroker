using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NymBroker.Core.DI;
using NymBroker.Core.Impl;
using NymBroker.Endpoint.SqlServer;
using NymBroker.SqlServerSample.Consumers;
using NymBroker.SqlServerSample.Messages;

// Start SQL Server first: ./scripts/setup-sqlserver.ps1
var sqlSettings = new SqlServerSettings
{
    ConnectionString = "Server=localhost,1433;Database=nymbroker;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True",
    TableName        = "dbo.orders",
    BatchSize        = 50,
    AutoCreateTable  = true
};

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((_, services) =>
    {
        services.AddNymBroker()
            .AddSqlServerEndPoint("SqlServerQueue", sqlSettings)
            .AddConsumer<OrderConsumer>()
            .Build();
    })
    .Build();

var broker = host.Services.GetRequiredService<INymBroker>();

// --- Post messages BEFORE starting the broker ---
// They are inserted as Status=Pending rows in SQL Server.
// The broker will claim and dispatch them once StartAsync is called.
Console.WriteLine("Inserting orders into SQL Server (Status=Pending)...");

await broker.PostAsync("SqlServerQueue", new OrderMessage("ORD-001", "Alice",  499.00m, "high"));
await broker.PostAsync("SqlServerQueue", new OrderMessage("ORD-002", "Bob",     29.99m, "normal"));
await broker.PostAsync("SqlServerQueue", new OrderMessage("ORD-003", "Carol", 1200.00m, "high"));

Console.WriteLine("3 orders written. Starting broker...");
Console.WriteLine();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

await host.StartAsync(cts.Token);

// Give the poll cycle one pass to process all pending rows.
await Task.Delay(500, cts.Token).ContinueWith(_ => { });

Console.WriteLine();
Console.WriteLine("Orders processed. Press Ctrl+C to stop.");

try { await Task.Delay(Timeout.Infinite, cts.Token); }
catch (OperationCanceledException) { }

await host.StopAsync();
Console.WriteLine("Broker stopped.");
