namespace MessageBroker.Core.Endpoint.HealthCheck;

public sealed class HealthCheckResult : IHealthCheckResult
{
    public bool IsHealthy { get; init; }
    public string? Message { get; init; }

    public static IHealthCheckResult Healthy() => new HealthCheckResult { IsHealthy = true };
    public static IHealthCheckResult Unhealthy(string message) => new HealthCheckResult { IsHealthy = false, Message = message };
}
