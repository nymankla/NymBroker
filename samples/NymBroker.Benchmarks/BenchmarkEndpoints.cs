using System.Net;
using System.Text;
using System.Text.Json;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Serialize;
using NymBroker.Core.Transform;

namespace NymBroker.Benchmarks;

/// <summary>Send-only endpoint with no transport cost, so a scenario measures only the broker's work to get there.</summary>
public sealed class SinkEndPoint(CompletionTracker tracker, bool signal) : IEndPoint
{
    public EndpointMode Mode => EndpointMode.WriteOnly;

    public Task PostAsync(byte[] message, CancellationToken ct = default)
    {
        if (signal) tracker.Signal();
        return Task.CompletedTask;
    }

    public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
}

/// <summary>A Memory queue without <see cref="IDeferredEndPoint"/>, so deferred posts wait in the broker, as on RabbitMQ.</summary>
public sealed class NonDeferringEndPoint(string name) : IEndPointEventDriven
{
    private readonly MemoryQueueEndPoint _queue = new(name);

    public Task PostAsync(byte[] message, CancellationToken ct = default) => _queue.PostAsync(message, ct);

    public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct)
        => _queue.StartListeningAsync(handler, ct);

    public Task StopListeningAsync() => _queue.StopListeningAsync();

    public IHealthCheckResult HealthCheck() => _queue.HealthCheck();
}

/// <summary>Answers every request at once, so the HTTP scenario measures the endpoint and client, not a server or a network.</summary>
public sealed class CountingHandler(CompletionTracker tracker) : HttpMessageHandler
{
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        tracker.Signal();
        return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
    }
}

/// <summary>Turns a bare JSON message, as from a system that knows nothing of envelopes, into a broker message.</summary>
public sealed class BareJsonInputTransformer : IInputTransformer
{
    public RawMessageContext? Transform(ReadOnlySpan<byte> input, string? sourceEndpoint) => new()
    {
        Id = Guid.NewGuid(),
        CorrelationId = Guid.NewGuid(),
        MessageType = "benchmark.message",
        Created = DateTime.UtcNow,
        RawMessage = JsonSerializer.Deserialize<JsonElement>(input)
    };
}

/// <summary>Sends only the message itself instead of the envelope, as for a destination with its own format.</summary>
public sealed class BareJsonOutputTransformer : IOutputTransformer
{
    public byte[]? Transform(RawMessageContext message, string destinationEndpoint)
        => Encoding.UTF8.GetBytes(message.RawMessage.GetRawText());
}
