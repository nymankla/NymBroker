using System.Collections.Immutable;

namespace NymBroker.Core.Endpoint.HealthCheck;

/// <summary>
/// Options for <c>INymBroker.CheckHealthAsync</c>, set with <c>NymBrokerBuilder.ConfigureHealthCheck</c>.
/// </summary>
public sealed class BrokerHealthCheckOptions
{
    /// <summary>Default for <see cref="Timeout"/>.</summary>
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);

    private TimeSpan _timeout = DefaultTimeout;
    private ImmutableHashSet<string> _nonCritical = ImmutableHashSet.Create<string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Overall time limit for one health check (default 10 s). Endpoints are checked in parallel; one that has not
    /// answered by then is reported unhealthy ("health check timed out").
    /// </summary>
    public TimeSpan Timeout
    {
        get => _timeout;
        set
        {
            if (value <= TimeSpan.Zero && value != System.Threading.Timeout.InfiniteTimeSpan)
                throw new ArgumentOutOfRangeException(nameof(value), value, "Health check timeout must be positive.");
            _timeout = value;
        }
    }

    /// <summary>Names of the endpoints marked non-critical (case-insensitive).</summary>
    public IReadOnlySet<string> NonCriticalEndpoints => _nonCritical;

    /// <summary>
    /// Marks endpoints as non-critical: when one of them is unhealthy and every critical endpoint is healthy, the
    /// broker is reported <see cref="BrokerHealthStatus.Degraded"/> instead of <see cref="BrokerHealthStatus.Unhealthy"/>.
    /// All endpoints are critical by default.
    /// </summary>
    public BrokerHealthCheckOptions NonCritical(params string[] endpointNames)
    {
        ArgumentNullException.ThrowIfNull(endpointNames);
        foreach (var name in endpointNames)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(name, nameof(endpointNames));
            _nonCritical = _nonCritical.Add(name);
        }
        return this;
    }

    /// <summary>Whether a failure of the named endpoint makes the whole broker unhealthy.</summary>
    public bool IsCritical(string endpointName) => !_nonCritical.Contains(endpointName);
}
