using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Route;
using NymBroker.Endpoint.AwsSqs;

// Start ElasticMQ (an SQS-compatible server) first: ./scripts/setup-sqs.ps1
// Runs two scenarios, checks each one, and exits 0 or 1. The queues are created if missing and purged at start.
// Against AWS: drop ServiceUrl and Credentials, set Region, and let the SDK's default chain (an IAM role) supply
// credentials — see docs/security.md. Create the queues and the redrive policy with your infrastructure tooling there.
var serviceUrl = Environment.GetEnvironmentVariable("NYMBROKER_SQS_URL") ?? "http://localhost:9324";
var credentials = new BasicAWSCredentials("local", "local");   // ElasticMQ accepts any key

using var admin = new AmazonSQSClient(credentials, new AmazonSQSConfig { ServiceURL = serviceUrl, AuthenticationRegion = "us-east-1" });
var ordersUrl = await CreateQueueAsync("nymbroker-sample", redriveTo: await CreateQueueAsync("nymbroker-sample-dlq"));
var failedUrl = await CreateQueueAsync("nymbroker-sample-failed");
var fifoUrl   = await CreateQueueAsync("nymbroker-sample.fifo", fifo: true);

var failures = 0;
void Check(bool ok, string what) { Console.WriteLine($"  [{(ok ? "ok" : "FAIL")}] {what}"); if (!ok) failures++; }

// --- 1. A failing order goes to an SQS "failed" queue, with the reason, and is read back for repair ----------------
// SQS cannot dead-letter a message on request, so the broker does it: WithDeadLetterEndpoint points at a second SQS
// queue. The redrive policy on nymbroker-sample (3 receives → nymbroker-sample-dlq) is the safety net for messages
// that keep coming back as Retry, e.g. when a route's destination is down.
Console.WriteLine("1. Orders on a standard queue; ORD-2 fails and is dead-lettered to nymbroker-sample-failed");
using (var host = BuildHost(services => services.AddNymBroker()
           .AddAwsSqsEndPoint("Orders", Settings(ordersUrl))
           .AddAwsSqsEndPoint("Failed", Settings(failedUrl))   // also listened to: OrderFailed repairs what lands here
           .WithDeadLetterEndpoint("Failed")
           .AddConsumer<OrderConsumer>()
           // Orders read back from the failed queue go to OrderFailed. A matched topic skips OrderConsumer.
           .AddTopic<OrderMessage>("orders.failed")
               .When(new FromRouteCondition("Failed"))
               .SubscribeWith<OrderFailed>()
               .Build()
           .Build()))
{
    await host.StartAsync();
    var broker = host.Services.GetRequiredService<INymBroker>();

    await broker.PostAsync("Orders", new OrderMessage("ORD-1", "alice", 499.00m));
    await broker.PostAsync("Orders", new OrderMessage("ORD-2", "bob", -1m));   // invalid: the consumer throws
    await broker.PostAsync("Orders", new OrderMessage("ORD-3", "carol", 29.99m));

    await WaitUntilAsync(() => OrderConsumer.Received.Count == 2 && !OrderFailed.Received.IsEmpty);
    await host.StopAsync();
}

var repaired = OrderFailed.Received.Single();
Check(OrderConsumer.Received.Select(r => r.OrderId).Order().SequenceEqual(["ORD-1", "ORD-3"]), "ORD-1 and ORD-3 processed by OrderConsumer");
Check(repaired.OrderId == "ORD-2" && repaired.DeadLetter?.Reason == DeadLetterReasons.ConsumerFailed,
      $"ORD-2 was routed from the failed queue to OrderFailed with its reason: {repaired.DeadLetter?.Reason} – {repaired.DeadLetter?.Description}");
Check(await CountAsync(ordersUrl) == 0 && await CountAsync(failedUrl) == 0, "both queues are empty: handled messages were deleted");

// --- 2. FIFO: one customer's orders stay in order, and a producer retry is delivered once --------------------------
Console.WriteLine("2. FIFO queue: per-customer order and deduplication");
OrderConsumer.Received.Clear();
using (var host = BuildHost(services => services.AddNymBroker()
           .AddAwsSqsEndPoint("FifoOrders", Settings(fifoUrl))
           .AddConsumer<OrderConsumer>()
           .Build()))
{
    await host.StartAsync();
    var broker = host.Services.GetRequiredService<INymBroker>();

    // The FIFO message group is the envelope's correlation id: one id per customer keeps that customer's orders in
    // sequence, while other customers' groups can be processed in parallel by other consumers.
    var dave = Guid.NewGuid();
    await broker.PostBatchAsync("FifoOrders", Enumerable.Range(1, 5).Select(i => new OrderMessage($"DAVE-{i}", "dave", i * 10m)), correlationId: dave);

    // The deduplication id is the envelope's id: posting the same envelope twice — what a producer retry after a
    // timeout looks like — is delivered once.
    var envelope = Envelope(Guid.NewGuid(), Guid.NewGuid(), new OrderMessage("EVE-1", "eve", 5m));
    await broker.PostRawAsync("FifoOrders", envelope);
    await broker.PostRawAsync("FifoOrders", envelope);

    await WaitUntilAsync(() => OrderConsumer.Received.Count >= 6);
    await Task.Delay(1500);   // give a duplicate the chance to show up
    await host.StopAsync();
}

Check(OrderConsumer.Received.Where(r => r.OrderId.StartsWith("DAVE")).Select(r => r.OrderId)
          .SequenceEqual(Enumerable.Range(1, 5).Select(i => $"DAVE-{i}")), "dave's 5 orders arrived in the order they were posted");
Check(OrderConsumer.Received.Count(r => r.OrderId == "EVE-1") == 1, "EVE-1 was posted twice and delivered once");

Console.WriteLine(failures == 0 ? "Demo passed." : $"Demo failed: {failures} check(s).");
return failures == 0 ? 0 : 1;

// --- helpers --------------------------------------------------------------------------------------------------------

AwsSqsSettings Settings(string queueUrl) => new()
{
    QueueUrl        = queueUrl,
    ServiceUrl      = serviceUrl,
    Credentials     = credentials,
    WaitTimeSeconds = 1   // short long-poll so the sample stops quickly; keep the default 20 in production
};

IHost BuildHost(Action<IServiceCollection> configure)
    => Host.CreateDefaultBuilder(args)
        .ConfigureLogging(l => l.SetMinimumLevel(LogLevel.Warning).AddFilter("OrderConsumer", LogLevel.Information).AddFilter("OrderFailed", LogLevel.Information))
        .ConfigureServices((_, services) => configure(services))
        .Build();

async Task<string> CreateQueueAsync(string name, string? redriveTo = null, bool fifo = false)
{
    var attributes = new Dictionary<string, string>();
    if (fifo) attributes["FifoQueue"] = "true";
    if (redriveTo is not null)
    {
        var arn = (await admin.GetQueueAttributesAsync(new GetQueueAttributesRequest(redriveTo, ["QueueArn"]))).QueueARN;
        attributes["RedrivePolicy"] = $$"""{"maxReceiveCount":"3","deadLetterTargetArn":"{{arn}}"}""";
    }
    var url = (await admin.CreateQueueAsync(new CreateQueueRequest(name) { Attributes = attributes })).QueueUrl;
    await admin.PurgeQueueAsync(url);   // AWS allows one purge per queue per 60 s; ElasticMQ has no limit
    return url;
}

async Task<int> CountAsync(string queueUrl)
{
    var a = await admin.GetQueueAttributesAsync(new GetQueueAttributesRequest(queueUrl, ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]));
    return a.ApproximateNumberOfMessages + a.ApproximateNumberOfMessagesNotVisible;
}

static async Task WaitUntilAsync(Func<bool> condition)
{
    var deadline = DateTime.UtcNow.AddSeconds(30);
    while (!condition())
    {
        if (DateTime.UtcNow > deadline) throw new TimeoutException("The expected state was not reached in 30 s.");
        await Task.Delay(100);
    }
}

// A NymBroker envelope with a fixed id, so two posts look like one message sent twice.
static byte[] Envelope(Guid id, Guid correlationId, OrderMessage order) => Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new
{
    id,
    correlationId,
    messageType = "orders.order-created",
    created = DateTime.UtcNow,
    message = order
}, JsonSerializerOptions.Web));

[MessageName("orders.order-created")]
public sealed record OrderMessage(string OrderId = "", string Customer = "", decimal Amount = 0m);

public sealed class OrderConsumer(ILogger<OrderConsumer> logger) : IConsume<OrderMessage>
{
    public sealed record Delivery(string Endpoint, string OrderId, DeadLetterInfo? DeadLetter);

    public static readonly ConcurrentQueue<Delivery> Received = new();

    public Task ConsumeAsync(OrderMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        if (msg.Amount < 0)
            throw new InvalidOperationException($"Order {msg.OrderId} has a negative amount");

        Received.Enqueue(new Delivery(ctx.Address?.From ?? "?", msg.OrderId, null));

        logger.LogInformation("Order {Id} for {Customer} processed ({Amount})", msg.OrderId, msg.Customer, msg.Amount);
        return Task.CompletedTask;
    }
}

// Receives the orders read back from the failed queue (see the "orders.failed" topic).
public sealed class OrderFailed(ILogger<OrderFailed> logger) : ISubscribe<OrderMessage>
{
    public static readonly ConcurrentQueue<OrderConsumer.Delivery> Received = new();

    public Task ReceiveAsync(OrderMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        // The reason travels in the envelope. Returning normally deletes the message from the failed queue.
        logger.LogInformation("[failed queue] Order {Id} needs repair: {Reason} – {Description}",
            msg.OrderId, ctx.DeadLetter?.Reason, ctx.DeadLetter?.Description);
        Received.Enqueue(new OrderConsumer.Delivery(ctx.Address?.From ?? "?", msg.OrderId, ctx.DeadLetter));
        return Task.CompletedTask;
    }
}
