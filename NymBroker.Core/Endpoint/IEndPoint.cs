using NymBroker.Core.Endpoint.HealthCheck;

namespace NymBroker.Core.Endpoint;

public interface IEndPoint
{
    EndpointMode Mode => EndpointMode.ReadWrite;
    Task PostAsync(Stream message, CancellationToken ct = default);
    IHealthCheckResult HealthCheck();
}
