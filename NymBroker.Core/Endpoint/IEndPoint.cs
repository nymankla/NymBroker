using NymBroker.Core.Endpoint.HealthCheck;

namespace NymBroker.Core.Endpoint;

public interface IEndPoint
{
    EndpointMode Mode => EndpointMode.ReadWrite;
    Task PostAsync(byte[] message, CancellationToken ct = default);
    IHealthCheckResult HealthCheck();

    /// <summary>
    /// Checks the endpoint without blocking a thread; the broker's <c>CheckHealthAsync</c> calls this. The default runs
    /// <see cref="HealthCheck"/>. Endpoints that contact a server override it, typically with
    /// <see cref="HealthCheckResult.FromProbeAsync"/>, and implement <see cref="HealthCheck"/> by waiting for it.
    /// </summary>
    Task<IHealthCheckResult> HealthCheckAsync(CancellationToken ct = default) => Task.FromResult(HealthCheck());

    /// <summary>
    /// Sends several envelopes, in order. The default posts them one by one; transports that can send many messages
    /// per round trip or transaction override it. Each element is a complete envelope and is received on its own.
    /// </summary>
    async Task PostBatchAsync(IReadOnlyList<byte[]> messages, CancellationToken ct = default)
    {
        foreach (var message in messages)
            await PostAsync(message, ct);
    }
}
