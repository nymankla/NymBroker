using NymBroker.Core.DI;
using NymBroker.Sql;
using NymBroker.Postgres;
using NymBroker.RabbitMq;
using NymBroker.ConsumerSample.Consumers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var transport = GetArg(args, "--transport", "sqlite");

var dbDir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "nymbrokersample");
Directory.CreateDirectory(dbDir);
var sqliteConnectionString  = $"Data Source={Path.Combine(dbDir, "nymbroker-queue.db")}";
const string PgConnectionString = "Host=localhost;Database=nymbroker;Username=postgres;Password=postgres";

var destination = transport switch
{
    "postgres" => $"postgres ({PgConnectionString})",
    "rabbit"   => "rabbitmq (localhost, queue: orders)",
    _          => sqliteConnectionString
};

Console.WriteLine("NymBroker Consumer Sample");
Console.WriteLine($"Transport : {transport}");
Console.WriteLine($"Source    : {destination}");
Console.WriteLine("Press Ctrl+C to stop.");
Console.WriteLine();

var host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(l => l.SetMinimumLevel(LogLevel.Information))
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
                    AutoCreateTable  = true,
                    PollInterval     = TimeSpan.FromMilliseconds(100),
                    MaxRetryCount    = 3,
                    BatchSize        = 500
                });
                break;

            case "rabbit":
                broker.AddRabbitMqEndPoint("Queue", new RabbitMqSettings
                {
                    HostName      = "localhost",
                    ReadQueueName = "orders"
                });
                break;

            default:
                broker.AddSqliteEndPoint("Queue", new SqliteSettings
                {
                    ConnectionString = sqliteConnectionString,
                    TableName        = "Orders",
                    AutoCreateTable  = true,
                    PollInterval     = TimeSpan.FromMilliseconds(500),
                    MaxRetryCount    = 3,
                    BatchSize        = 100
                });
                break;
        }

        broker.AddConsumer<OrderConsumer>().Build();
    })
    .Build();

await host.RunAsync();

// --- helpers ---

static string GetArg(string[] args, string flag, string fallback)
{
    var idx = Array.IndexOf(args, flag);
    return idx >= 0 && idx + 1 < args.Length ? args[idx + 1].ToLowerInvariant() : fallback;
}
