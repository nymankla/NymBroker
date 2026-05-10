using NymBroker.Core.Endpoint.HealthCheck;

namespace NymBroker.Core.Endpoint;

public interface IEndPoint
{
    string Name { get; }
    EndpointMode Mode => EndpointMode.ReadWrite;
    Task PostAsync(Stream message, CancellationToken ct = default);
    IHealthCheckResult HealthCheck();
}
