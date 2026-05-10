using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Impl;
using NymBroker.Sql;
using NymBroker.Postgres;
using NymBroker.RabbitMq;
using NymBroker.ProducerSample.Messages;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var transport = GetArg(args, "--transport", "sqlite");
var count     = GetCount(args);

var dbDir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "nymbrokersample");
Directory.CreateDirectory(dbDir);
var sqliteConnectionString  = $"Data Source={Path.Combine(dbDir, "nymbroker-queue.db")}";
const string PgConnectionString = "Host=localhost;Database=nymbroker;Username=postgres;Password=postgres";

var host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(l => l.SetMinimumLevel(LogLevel.Warning))
    .ConfigureServices((_, services) =>
    {
        var broker = services.AddNymBroker();

        switch (transport)
        {
            case "postgres":
                broker.AddPostgresEndPoint("Queue", new PostgresSettings
                {
                    ConnectionString = PgConnectionString,
                    TableName        = "orders",
                    AutoCreateTable  = true
                }, EndpointMode.WriteOnly);
                break;

            case "rabbit":
                broker.AddRabbitMqEndPoint("Queue", new RabbitMqSettings
                {
                    HostName       = "localhost",
                    WriteQueueName = "orders"
                }, EndpointMode.WriteOnly);
                break;

            default:
                broker.AddSqliteEndPoint("Queue", new SqliteSettings
                {
                    ConnectionString = sqliteConnectionString,
                    TableName        = "Orders",
                    AutoCreateTable  = true
                }, EndpointMode.WriteOnly);
                break;
        }

        broker.Build();
    })
    .Build();

await host.StartAsync();

var brokerService = host.Services.GetRequiredService<INymBroker>();
var customers     = new[] { "Alice", "Bob", "Carol", "Dave", "Eve" };
var destination   = transport switch
{
    "postgres" => $"postgres ({PgConnectionString})",
    "rabbit"   => "rabbitmq (localhost, queue: orders)",
    _          => sqliteConnectionString
};

Console.WriteLine($"Transport : {transport}");
Console.WriteLine($"Target    : {destination}");
Console.WriteLine($"Posting {count} order(s)...");
Console.WriteLine();

for (var i = 0; i < count; i++)
{
    var message = new OrderMessage(
        OrderId:  $"ORD-{DateTime.UtcNow:yyyyMMddHHmmss}-{i + 1:000}",
        Customer: customers[i % customers.Length],
        Amount:   Math.Round((i + 1) * 9.99m, 2),
        Priority: i % 3 == 0 ? "high" : "normal");

    await brokerService.PostAsync("Queue", message);
    Console.WriteLine($"  [{i + 1}/{count}] {message.OrderId} | {message.Customer} | {message.Amount:C} | {message.Priority}");
}

Console.WriteLine();
Console.WriteLine($"Done. {count} message(s) written.");

await host.StopAsync();

// --- helpers ---

static string GetArg(string[] args, string flag, string fallback)
{
    var idx = Array.IndexOf(args, flag);
    return idx >= 0 && idx + 1 < args.Length ? args[idx + 1].ToLowerInvariant() : fallback;
}

static int GetCount(string[] args)
{
    foreach (var a in args)
        if (!a.StartsWith('-') && int.TryParse(a, out var n) && n > 0)
            return n;
    return 1;
}
