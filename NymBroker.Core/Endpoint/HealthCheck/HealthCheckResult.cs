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
    /// Runs a connectivity probe for <see cref="IEndPoint.HealthCheckAsync"/>: healthy when <paramref name="probe"/>
    /// completes within <paramref name="timeout"/> (default <see cref="DefaultProbeTimeout"/>); otherwise the failure
    /// is logged at Error and returned as unhealthy with its message. Never throws, except for cancellation of
    /// <paramref name="cancellationToken"/>.
    /// </summary>
    /// <param name="endpointKind">Transport name for the log message, e.g. "PostgreSQL".</param>
    public static async Task<IHealthCheckResult> FromProbeAsync(string endpointKind, string endpointName, ILogger logger,
        Func<CancellationToken, Task> probe, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(timeout ?? DefaultProbeTimeout);
        try
        {
            await probe(cts.Token).ConfigureAwait(false);
            return Healthy();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;   // the caller gave up; not an endpoint failure
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "{EndpointKind} endpoint '{Name}' health check failed", endpointKind, endpointName);
            return Unhealthy(ex.Message);
        }
    }

    /// <summary>
    /// The blocking form of <see cref="FromProbeAsync"/> for a synchronous <see cref="IEndPoint.HealthCheck"/>. It holds
    /// the calling thread until the probe finishes; prefer implementing <see cref="IEndPoint.HealthCheckAsync"/>.
    /// </summary>
    public static IHealthCheckResult FromProbe(string endpointKind, string endpointName, ILogger logger,
        Func<CancellationToken, Task> probe, TimeSpan? timeout = null)
        => FromProbeAsync(endpointKind, endpointName, logger, probe, timeout).GetAwaiter().GetResult();
}
