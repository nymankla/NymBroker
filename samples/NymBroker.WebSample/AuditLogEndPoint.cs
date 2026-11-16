using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;

namespace NymBroker.WebSample;

/// <summary>
/// A send-only endpoint that only counts what it receives: the target of the sample's audit route. A write-only Memory
/// endpoint would fill its bounded channel once nothing reads it, and then hold up the route that posts to it.
/// </summary>
public sealed class AuditLogEndPoint(ILogger<AuditLogEndPoint> logger) : IEndPoint
{
    private long _received;

    public EndpointMode Mode => EndpointMode.WriteOnly;

    public Task PostAsync(byte[] message, CancellationToken ct = default)
    {
        var count = Interlocked.Increment(ref _received);
        if (count % 1000 == 0) logger.LogInformation("Audit log has received {Count} high-priority orders", count);
        return Task.CompletedTask;
    }

    public IHealthCheckResult HealthCheck()
        => new HealthCheckResult { IsHealthy = true, Message = $"{Interlocked.Read(ref _received)} orders logged" };
}
