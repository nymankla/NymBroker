namespace MessageBroker.Core.Endpoint.HealthCheck;

public interface IHealthCheckResult
{
    bool IsHealthy { get; }
    string? Message { get; }
}
