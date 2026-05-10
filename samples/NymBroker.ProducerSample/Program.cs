using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NymBroker.Core.DI;
using NymBroker.Core.Impl;
using NymBroker.Postgres;
using NymBroker.ProducerSample.Messages;
using NymBroker.RabbitMq;
using NymBroker.Sql;

var transport = args.SkipWhile(a => a != "--transport").Skip(1).FirstOrDefault() ?? "sqlite";

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
                });
                break;

            case "rabbitmq":
                builder.AddRabbitMqEndPoint("Queue", new RabbitMqSettings
                {
                    HostName       = "localhost",
                    WriteQueueName = "consumer.sample"
                });
                break;

            default: // sqlite
                builder.AddSqliteEndPoint("Queue", new SqliteSettings
                {
                    ConnectionString = "Data Source=consumer-sample.db",
                    AutoCreateTable  = true
                });
                break;
        }

        builder.Build();
    })
    .Build();

var broker = host.Services.GetRequiredService<INymBroker>();

await host.StartAsync();

Console.WriteLine($"ProducerSample: posting 3 orders via {transport}...");

await broker.PostAsync("Queue", new OrderMessage("ORD-001", "Alice",  499.00m, "high"));
await broker.PostAsync("Queue", new OrderMessage("ORD-002", "Bob",     29.99m, "normal"));
await broker.PostAsync("Queue", new OrderMessage("ORD-003", "Carol", 1200.00m, "high"));

Console.WriteLine("3 orders posted.");

await host.StopAsync();
