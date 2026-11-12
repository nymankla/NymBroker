using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Factory.Configuration;

var configPath = Path.Combine(Path.GetTempPath(), $"nymbroker-aot-{Guid.NewGuid():N}.json");
try
{
    await File.WriteAllTextAsync(configPath, "{\"NymBroker\":{\"endpoints\":[{\"name\":\"configured\",\"type\":\"Memory\",\"config\":{\"capacity\":12}}]}}");
    var configuration = BrokerConfigurationReader.Read(configPath);
    var configuredEndpoint = configuration.Endpoints.Single();
    if (configuredEndpoint.Name != "configured" || configuredEndpoint.Config?.GetProperty("capacity").GetInt32() != 12)
        return 1;
}
finally
{
    File.Delete(configPath);
}

var deadLetterSink = new CapturingEndPoint();
var services = new ServiceCollection();
services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Warning));
services.AddKeyedSingleton<IEndPoint>("dead-letters", deadLetterSink);
var brokerBuilder = services.AddNymBroker()
    .AddMemoryEndPoint("messages")
    .AddJsonSerializerContext(AotJsonContext.Default)
    .AddConsumer<AotMessageConsumer, AotMessage>()
    .AddConsumer<AotFailureConsumer, AotFailureMessage>(options => options.RetryOnFailure = false)
    .WithDeadLetterEndpoint("dead-letters");
brokerBuilder.RegisterEndpoint("dead-letters");
brokerBuilder.Build();

await using var provider = services.BuildServiceProvider();
var broker = provider.GetRequiredService<INymBroker>();
await broker.StartAsync();
await broker.PostAsync("messages", new AotMessage("native-aot"));

var received = await AotMessageConsumer.Received.Task.WaitAsync(TimeSpan.FromSeconds(10));
await broker.PostAsync("messages", new AotFailureMessage());
var deadLetterBytes = await deadLetterSink.Message.Task.WaitAsync(TimeSpan.FromSeconds(10));
using var deadLetter = JsonDocument.Parse(deadLetterBytes);
var reason = deadLetter.RootElement.GetProperty("deadLetter").GetProperty("reason").GetString();
await broker.StopAsync();
if (received != "native-aot" || reason != "ConsumerFailed") return 1;

Console.WriteLine("Native AOT configuration, message round-trip, and dead-letter checks passed.");
return 0;

public sealed record AotMessage(string Value);

public sealed class AotMessageConsumer : IConsume<AotMessage>
{
    public static TaskCompletionSource<string> Received { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task ConsumeAsync(AotMessage message, NymBroker.Core.Message.IMessageContext context, CancellationToken ct = default)
    {
        Received.TrySetResult(message.Value);
        return Task.CompletedTask;
    }
}

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, PropertyNameCaseInsensitive = true)]
[JsonSerializable(typeof(AotMessage))]
[JsonSerializable(typeof(AotFailureMessage))]
internal partial class AotJsonContext : JsonSerializerContext;

public sealed record AotFailureMessage;

public sealed class AotFailureConsumer : IConsume<AotFailureMessage>
{
    public Task ConsumeAsync(AotFailureMessage message, NymBroker.Core.Message.IMessageContext context, CancellationToken ct = default)
        => Task.FromException(new InvalidOperationException("Expected AOT smoke-test failure."));
}

public sealed class CapturingEndPoint : IEndPoint
{
    public EndpointMode Mode => EndpointMode.WriteOnly;
    public TaskCompletionSource<byte[]> Message { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task PostAsync(byte[] message, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        Message.TrySetResult(message);
        return Task.CompletedTask;
    }

    public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
}
