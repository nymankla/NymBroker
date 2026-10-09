using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Endpoint.HealthCheck;

public sealed class HealthCheckResult : IHealthCheckResult
{
    /// <summary>The timeout endpoints use for their own connectivity probe.</summary>
    public static readonly TimeSpan DefaultProbeTimeout = TimeSpan.FromSeconds(5);

    public bool IsHealthy { get; init; }
    public string? Message { get; init; }

    public static IHealthCheckResult Healthy() => new HealthCheckResult { IsHealthy = true };
    public static IHealthCheckResult Unhealthy(string message) => new HealthCheckResult { IsHealthy = false, Message = message };

    /// <summary>
    /// Runs an asynchronous connectivity probe for the synchronous <see cref="IEndPoint.HealthCheck"/>: healthy when
    /// <paramref name="probe"/> completes within <paramref name="timeout"/> (default <see cref="DefaultProbeTimeout"/>);
    /// otherwise the failure is logged at Error and returned as unhealthy with its message. Never throws.
    /// </summary>
    /// <param name="endpointKind">Transport name for the log message, e.g. "PostgreSQL".</param>
    public static IHealthCheckResult FromProbe(string endpointKind, string endpointName, ILogger logger,
        Func<CancellationToken, Task> probe, TimeSpan? timeout = null)
    {
        try
        {
            using var cts = new CancellationTokenSource(timeout ?? DefaultProbeTimeout);
            probe(cts.Token).GetAwaiter().GetResult();
            return Healthy();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "{EndpointKind} endpoint '{Name}' health check failed", endpointKind, endpointName);
            return Unhealthy(ex.Message);
        }
    }
}
