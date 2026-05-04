using MessageBroker.Core.Endpoint.HealthCheck;

namespace MessageBroker.Core.Endpoint;

public interface IEndPoint
{
    string Name { get; }
    Task PostAsync(Stream message, CancellationToken ct = default);
    IHealthCheckResult HealthCheck();
}
