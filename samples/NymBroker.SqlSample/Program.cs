using NymBroker.Core.DI;
using NymBroker.Core.Impl;
using NymBroker.Sql;
using NymBroker.SqlSample.Consumers;
using NymBroker.SqlSample.Messages;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// Use a file-based SQLite database so messages survive process restarts.
// Delete any leftover DB from a previous run for a clean demo.
const string DbPath = "sqlsample.db";
if (File.Exists(DbPath)) File.Delete(DbPath);

var sqlSettings = new SqliteSettings
{
    ConnectionString = $"Data Source={DbPath}",
    TableName        = "Orders",
    BatchSize        = 50,
    AutoCreateTable  = true
};

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((_, services) =>
    {
        services.AddNymBroker()
            .AddSqliteEndPoint("SqlQueue", sqlSettings)
            .AddConsumer<OrderConsumer>()
            .Build();
    })
    .Build();

var broker = host.Services.GetRequiredService<INymBroker>();

// --- Post messages BEFORE starting the broker ---
// They land in the SQLite DB as Status='Pending'.
// The broker will claim and dispatch them once StartAsync is called.
Console.WriteLine("Inserting orders into SQLite (Status=Pending)...");

await broker.PostAsync("SqlQueue", new OrderMessage("ORD-001", "Alice",  499.00m, "high"));
await broker.PostAsync("SqlQueue", new OrderMessage("ORD-002", "Bob",     29.99m, "normal"));
await broker.PostAsync("SqlQueue", new OrderMessage("ORD-003", "Carol", 1200.00m, "high"));

Console.WriteLine("3 orders written to DB. Starting broker...");
Console.WriteLine();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

await host.StartAsync(cts.Token);

// Give the poll cycle one pass to process all pending rows.
await Task.Delay(500, cts.Token).ContinueWith(_ => { });

Console.WriteLine();
Console.WriteLine("Orders processed — rows in DB are now Status=Processed.");
Console.WriteLine("Press Ctrl+C to stop.");

try { await Task.Delay(Timeout.Infinite, cts.Token); }
catch (OperationCanceledException) { }

await host.StopAsync();
Console.WriteLine("Broker stopped.");
