namespace NymBroker.Core.Endpoint.HealthCheck;

/// <summary>Health of the broker as a whole, or of one endpoint.</summary>
public enum BrokerHealthStatus
{
    Healthy,
    /// <summary>Only endpoints marked non-critical (<see cref="BrokerHealthCheckOptions.NonCritical"/>) are unhealthy.</summary>
    Degraded,
    Unhealthy
}

/// <summary>Result of one endpoint's <see cref="IEndPoint.HealthCheck"/> inside a <see cref="BrokerHealthReport"/>.</summary>
/// <param name="Name">The endpoint's registered name.</param>
/// <param name="Status"><see cref="BrokerHealthStatus.Healthy"/> or <see cref="BrokerHealthStatus.Unhealthy"/>.</param>
/// <param name="Message">The endpoint's message, the exception message if the check threw, or a timeout message.</param>
/// <param name="Mode">The endpoint's <see cref="EndpointMode"/>.</param>
/// <param name="Duration">How long the check took (up to the overall timeout if it did not answer in time).</param>
/// <param name="IsCritical">False when the endpoint is marked non-critical: its failure makes the broker Degraded, not Unhealthy.</param>
public sealed record EndpointHealth(
    string Name,
    BrokerHealthStatus Status,
    string? Message,
    EndpointMode Mode,
    TimeSpan Duration,
    bool IsCritical = true)
{
    public bool IsHealthy => Status == BrokerHealthStatus.Healthy;
}

/// <summary>Aggregated health of the broker and every registered endpoint, from <c>INymBroker.CheckHealthAsync</c>.</summary>
/// <param name="Status">The aggregate: Unhealthy if the broker is not running or a critical endpoint is unhealthy,
/// Degraded if only non-critical endpoints are unhealthy, otherwise Healthy.</param>
/// <param name="BrokerStarted">Whether the broker has been started (and not stopped).</param>
/// <param name="Endpoints">One entry per registered endpoint, ordered by name.</param>
/// <param name="Duration">Total time taken by the check.</param>
/// <param name="Message">A short summary of what is wrong; null when healthy.</param>
public sealed record BrokerHealthReport(
    BrokerHealthStatus Status,
    bool BrokerStarted,
    IReadOnlyList<EndpointHealth> Endpoints,
    TimeSpan Duration,
    string? Message = null)
{
    public bool IsHealthy => Status == BrokerHealthStatus.Healthy;
}
