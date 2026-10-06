using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Impl;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using MsHealthCheckResult = Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckResult;

namespace NymBroker.Core.DI;

/// <summary>
/// <see cref="IHealthCheck"/> adapter for <see cref="INymBroker.CheckHealthAsync"/>, for ASP.NET Core health endpoints
/// and Kubernetes probes. Register it with <see cref="NymBrokerHealthChecksBuilderExtensions.AddNymBroker"/>.
/// </summary>
/// <remarks>
/// Healthy / Degraded map one to one; Unhealthy maps to the registration's failure status (Unhealthy by default).
/// <see cref="MsHealthCheckResult.Data"/> holds <c>"brokerStarted"</c> and one entry per endpoint,
/// <c>"&lt;name&gt;": "&lt;status&gt;[: &lt;message&gt;]"</c>.
/// </remarks>
public sealed class NymBrokerHealthCheck(INymBroker broker) : IHealthCheck
{
    public const string BrokerStartedKey = "brokerStarted";

    public async Task<MsHealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var report = await broker.CheckHealthAsync(cancellationToken).ConfigureAwait(false);

        var data = new Dictionary<string, object>(StringComparer.Ordinal);
        foreach (var endpoint in report.Endpoints)
            data[endpoint.Name] = string.IsNullOrEmpty(endpoint.Message)
                ? endpoint.Status.ToString()
                : $"{endpoint.Status}: {endpoint.Message}";
        data[BrokerStartedKey] = report.BrokerStarted;

        var status = report.Status switch
        {
            BrokerHealthStatus.Healthy  => HealthStatus.Healthy,
            BrokerHealthStatus.Degraded => HealthStatus.Degraded,
            _                           => context?.Registration?.FailureStatus ?? HealthStatus.Unhealthy
        };

        return new MsHealthCheckResult(status, report.Message, data: data);
    }
}

public static class NymBrokerHealthChecksBuilderExtensions
{
    /// <summary>
    /// Adds a health check that reports <see cref="INymBroker.CheckHealthAsync"/>: the broker's state and every
    /// registered endpoint. Requires the broker to be registered (<c>services.AddNymBroker()...Build()</c>).
    /// </summary>
    /// <param name="builder">The health checks builder from <c>services.AddHealthChecks()</c>.</param>
    /// <param name="name">The health check name.</param>
    /// <param name="failureStatus">Status reported when the broker is unhealthy; null means <see cref="HealthStatus.Unhealthy"/>.</param>
    /// <param name="tags">Tags for filtering, e.g. <c>["ready"]</c>.</param>
    /// <param name="timeout">Optional timeout of the health check framework itself; the broker's own timeout is
    /// <see cref="BrokerHealthCheckOptions.Timeout"/>.</param>
    public static IHealthChecksBuilder AddNymBroker(
        this IHealthChecksBuilder builder,
        string name = "nymbroker",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        return builder.Add(new HealthCheckRegistration(
            name,
            sp => new NymBrokerHealthCheck(sp.GetRequiredService<INymBroker>()),
            failureStatus,
            tags,
            timeout));
    }
}
