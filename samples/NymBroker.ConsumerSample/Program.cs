using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NymBroker.Core.DI;
using NymBroker.ConsumerSample.Consumers;
using NymBroker.Postgres;
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

            case "rabbit":
            case "rabbitmq":
                builder.AddRabbitMqEndPoint("Queue", new RabbitMqSettings
                {
                    HostName      = "localhost",
                    ReadQueueName = "consumer.sample"
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

        builder.AddConsumer<OrderConsumer>().Build();
    })
    .Build();

Console.WriteLine($"ConsumerSample started (transport: {transport}). Waiting for messages — Ctrl+C to stop.");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

await host.StartAsync(cts.Token);

try { await Task.Delay(Timeout.Infinite, cts.Token); }
catch (OperationCanceledException) { }

await host.StopAsync();
Console.WriteLine("ConsumerSample stopped.");
