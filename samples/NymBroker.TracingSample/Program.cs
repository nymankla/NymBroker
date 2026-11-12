using System.Diagnostics;
using NymBroker.Core;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Message;
using NymBroker.Endpoint.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

const string SampleSourceName = "NymBroker.TracingSample";
var modeIndex = Array.IndexOf(args, "--mode");
var mode = modeIndex < 0 ? "consumer" : modeIndex + 1 < args.Length ? args[modeIndex + 1] : "";
if (mode is not ("producer" or "consumer"))
    throw new ArgumentException("Use --mode producer or --mode consumer.", nameof(args));

using var listener = new ActivityListener
{
    ShouldListenTo = source => source.Name is NymBrokerActivitySource.InstrumentationName or SampleSourceName,
    Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
    SampleUsingParentId = (ref ActivityCreationOptions<string> _) => ActivitySamplingResult.AllDataAndRecorded,
    ActivityStopped = activity =>
    {
        Console.WriteLine($"Span: {activity.DisplayName}");
        Console.WriteLine($"  trace={activity.TraceId} span={activity.SpanId} parent={activity.ParentSpanId}");
        foreach (var tag in activity.TagObjects)
            Console.WriteLine($"  {tag.Key}={tag.Value}");
    }
};
ActivitySource.AddActivityListener(listener);

var databasePath = Path.Combine(Path.GetTempPath(), "nymbroker-tracing-sample.db");
using var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        var broker = services.AddNymBroker()
            .AddSqliteEndPoint("Orders", new SqliteSettings
            {
                ConnectionString = $"Data Source={databasePath}",
                TableName = "TracingSample",
                AutoCreateTable = true
            }, mode == "consumer" ? EndpointMode.ReadWrite : EndpointMode.WriteOnly);

        if (mode == "consumer")
            broker.AddConsumer<OrderConsumer>();

        broker.Build();
    })
    .Build();

if (mode == "consumer")
{
    await host.StartAsync();
    Console.WriteLine("Consumer listening for orders. Start the producer in another terminal; press Ctrl+C to stop.");
    await host.WaitForShutdownAsync();
    return;
}

await host.StartAsync();
var nymBroker = host.Services.GetRequiredService<INymBroker>();
using (var request = SampleActivitySource.Source.StartActivity("submit-order", ActivityKind.Server))
{
    if (request is null)
        throw new InvalidOperationException("The sample trace listener did not create the root activity.");

    var order = new OrderMessage("ORD-TRACE-001", "Alice");
    await nymBroker.PostAsync("Orders", order);
    Console.WriteLine($"Posted {order.OrderId}; trace={request.TraceId}");
}

await host.StopAsync();

static class SampleActivitySource
{
    public static readonly ActivitySource Source = new("NymBroker.TracingSample");
}

public sealed record OrderMessage(string OrderId, string Customer);

public sealed class OrderConsumer : IConsume<OrderMessage>
{
    public Task ConsumeAsync(OrderMessage message, IMessageContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine($"Handled {message.OrderId} for {message.Customer}; trace={Activity.Current?.TraceId}");
        return Task.CompletedTask;
    }
}
