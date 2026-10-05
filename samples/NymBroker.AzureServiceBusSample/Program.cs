using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NymBroker.AzureServiceBus;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;

// Start the emulator first: ./scripts/setup-servicebus.ps1
// Against Azure, use your namespace's connection string, or FullyQualifiedNamespace + Credential (e.g. DefaultAzureCredential).
const string connectionString = "Endpoint=sb://localhost:5673;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
const string queue = "nymbroker.sample";   // MaxDeliveryCount = 3 in scripts/servicebus/Config.json

await DrainAsync();   // start from an empty queue and dead-letter queue

// --- 1. Process orders; an invalid one fails every time and is dead-lettered by Service Bus -------------------
using (var host = BuildHost(services => services.AddNymBroker()
           .AddAzureServiceBusEndPoint("Orders", new AzureServiceBusSettings { ConnectionString = connectionString, QueueName = queue })
           .AddConsumer<OrderConsumer>()
           .Build()))
{
    await host.StartAsync();
    var broker = host.Services.GetRequiredService<INymBroker>();

    await broker.PostAsync("Orders", new OrderMessage("ORD-001", 499.00m));
    await broker.PostAsync("Orders", new OrderMessage("ORD-002", -1m));   // invalid: the consumer throws
    await broker.PostAsync("Orders", new OrderMessage("ORD-003", 29.99m));

    Console.WriteLine("Posted 3 orders. ORD-002 fails, is redelivered, and lands in the dead-letter queue after 3 deliveries...");
    await WaitForDeadLetterAsync();
    await host.StopAsync();
}

// --- 2. Service Bus recorded why it was dead-lettered ---------------------------------------------------------
await using (var client = new ServiceBusClient(connectionString))
await using (var dlqPeek = client.CreateReceiver(queue, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter }))
{
    foreach (var m in await dlqPeek.PeekMessagesAsync(10))
        Console.WriteLine($"Dead-letter queue: reason={m.DeadLetterReason}, deliveries={m.DeliveryCount}");
}

// --- 3. A second endpoint reads the dead-letter queue (repair / replay flows) ---------------------------------
using (var host = BuildHost(services => services.AddNymBroker()
           .AddAzureServiceBusEndPoint("OrdersDlq", new AzureServiceBusSettings
           {
               ConnectionString = connectionString,
               QueueName = queue,
               ReadDeadLetterQueue = true
           })
           .AddConsumer<OrderConsumer>()
           .Build()))
{
    await host.StartAsync();
    await Task.Delay(TimeSpan.FromSeconds(3));
    await host.StopAsync();
}

Console.WriteLine("Done.");

IHost BuildHost(Action<IServiceCollection> configure)
    => Host.CreateDefaultBuilder(args)
        .ConfigureLogging(l => l.SetMinimumLevel(LogLevel.Warning).AddFilter("OrderConsumer", LogLevel.Information))
        .ConfigureServices((_, services) => configure(services))
        .Build();

async Task WaitForDeadLetterAsync()
{
    await using var client = new ServiceBusClient(connectionString);
    await using var receiver = client.CreateReceiver(queue, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
    for (var i = 0; i < 60 && await receiver.PeekMessageAsync() is null; i++)
        await Task.Delay(500);
}

async Task DrainAsync()
{
    await using var client = new ServiceBusClient(connectionString);
    foreach (var subQueue in new[] { SubQueue.None, SubQueue.DeadLetter })
    {
        await using var receiver = client.CreateReceiver(queue, new ServiceBusReceiverOptions { SubQueue = subQueue });
        IReadOnlyList<ServiceBusReceivedMessage> batch;
        while ((batch = await receiver.ReceiveMessagesAsync(100, TimeSpan.FromMilliseconds(500))).Count > 0)
            foreach (var m in batch) await receiver.CompleteMessageAsync(m);
    }
}

[MessageName("order.created")]
public sealed record OrderMessage(string OrderId = "", decimal Amount = 0m);

public sealed class OrderConsumer(ILogger<OrderConsumer> logger) : IConsume<OrderMessage>
{
    public Task ConsumeAsync(OrderMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        if (ctx.Address?.From == "OrdersDlq")
        {
            // Read back from the dead-letter queue: log it for repair; returning normally removes it from the DLQ.
            logger.LogInformation("[DLQ] Order {Id} ({Amount}) taken out of the dead-letter queue for repair", msg.OrderId, msg.Amount);
            return Task.CompletedTask;
        }

        if (msg.Amount < 0)
            throw new InvalidOperationException($"Order {msg.OrderId} has a negative amount");

        logger.LogInformation("Order {Id} processed ({Amount})", msg.OrderId, msg.Amount);
        return Task.CompletedTask;
    }
}
