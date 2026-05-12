using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Impl;
using NymBroker.Postgres;
using NymBroker.ProducerSample.Messages;
using NymBroker.RabbitMq;
using NymBroker.Sql;

var transport = args.SkipWhile(a => a != "--transport").Skip(1).FirstOrDefault() ?? "sqlite";
var countArg  = args.SkipWhile(a => a != "--count").Skip(1).FirstOrDefault();
var count     = int.TryParse(countArg, out var n) && n > 0 ? n : 3;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((_, services) =>
    {
        var builder = services.AddNymBroker();

        switch (transport.ToLowerInvariant())
        {
            case "postgres":
                builder.AddPostgresEndPoint("Queue", new PostgresSettings
                {
                    ConnectionString = "Host=localhost;Database=nymbroker;Username=postgres;Password=postgres",
                    TableName        = "consumer_sample",
                    AutoCreateTable  = true
                }, EndpointMode.WriteOnly);
                break;

            case "rabbit":
            case "rabbitmq":
                builder.AddRabbitMqEndPoint("Queue", new RabbitMqSettings
                {
                    HostName       = "localhost",
                    WriteQueueName = "consumer.sample"
                }, EndpointMode.WriteOnly);
                break;

            default: // sqlite
                builder.AddSqliteEndPoint("Queue", new SqliteSettings
                {
                    ConnectionString = "Data Source=consumer-sample.db",
                    AutoCreateTable  = true
                }, EndpointMode.WriteOnly);
                break;
        }

        builder.Build();
    })
    .Build();

var broker = host.Services.GetRequiredService<INymBroker>();
var logger = host.Services.GetRequiredService<ILogger<Program>>();

await host.StartAsync();

Log.Starting(logger, transport, count);

var customers  = new[] { "Alice", "Bob", "Carol", "Dave", "Eve" };
var priorities = new[] { "high", "normal", "low" };
var rng        = new Random(42);

for (var i = 1; i <= count; i++)
{
    var id       = $"ORD-{i:D4}";
    var customer = customers[rng.Next(customers.Length)];
    var amount   = Math.Round(rng.NextDouble() * 1999 + 1, 2);
    var priority = priorities[rng.Next(priorities.Length)];
    await broker.PostAsync("Queue", new OrderMessage(id, customer, (decimal)amount, priority));
    Log.OrderPosted(logger, id, customer, amount, priority);
}

Log.BatchComplete(logger, count, transport);

await host.StopAsync();

static partial class Log
{
    [LoggerMessage(Level = LogLevel.Information, Message = "ProducerSample starting — transport={Transport} count={Count}")]
    public static partial void Starting(ILogger logger, string transport, int count);

    [LoggerMessage(Level = LogLevel.Information, Message = "Posted {OrderId} {Customer} {Amount:F2} [{Priority}]")]
    public static partial void OrderPosted(ILogger logger, string orderId, string customer, double amount, string priority);

    [LoggerMessage(Level = LogLevel.Information, Message = "{Count} order(s) posted via {Transport}")]
    public static partial void BatchComplete(ILogger logger, int count, string transport);
}
