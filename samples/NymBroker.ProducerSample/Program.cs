using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
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

await host.StartAsync();

Console.WriteLine($"ProducerSample: posting {count} orders via {transport}...");

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
}

Console.WriteLine($"{count} orders posted.");

await host.StopAsync();
