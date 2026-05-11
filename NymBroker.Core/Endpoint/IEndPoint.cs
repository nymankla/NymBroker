using NymBroker.Core.Endpoint.HealthCheck;

namespace NymBroker.Core.Endpoint;

public interface IEndPoint
{
    EndpointMode Mode => EndpointMode.ReadWrite;
    Task PostAsync(byte[] message, CancellationToken ct = default);
    IHealthCheckResult HealthCheck();
}
