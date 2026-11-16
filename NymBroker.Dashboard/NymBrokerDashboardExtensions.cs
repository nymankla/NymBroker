using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NymBroker.Dashboard.Api;
using NymBroker.Dashboard.Metrics;

namespace NymBroker.Dashboard;

/// <summary>Adds the NymBroker dashboard to an ASP.NET Core app.</summary>
public static class NymBrokerDashboardExtensions
{
    /// <summary>
    /// Registers the dashboard's services: the metric collector (started with the host, so the charts have history
    /// when the page opens) and the shared refresh loop. Call it once, next to <c>AddNymBroker()</c>.
    /// </summary>
    public static IServiceCollection AddNymBrokerDashboard(this IServiceCollection services, Action<NymBrokerDashboardOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (services.Any(s => s.ServiceType == typeof(NymBrokerDashboardOptions)))
            throw new InvalidOperationException("AddNymBrokerDashboard was called twice; call it once.");

        var options = new NymBrokerDashboardOptions();
        configure?.Invoke(options);
        options.Validate();

        services.AddSingleton(options);
        services.TryAddSingleton(TimeProvider.System);
        services.AddSingleton<MetricCollector>();
        services.AddSingleton<SnapshotService>();
        services.AddHostedService<DashboardMetricsService>();
        return services;
    }

    /// <summary>
    /// Serves the dashboard and its read-only JSON API under <paramref name="pattern"/> (default <c>/nymbroker</c>).
    /// Returns the route group, so <c>.RequireAuthorization(...)</c> and other conventions apply to all of it. Without an
    /// authorization policy it answers only requests from this machine (see <see cref="NymBrokerDashboardOptions.AllowAnonymousRemoteAccess"/>).
    /// </summary>
    public static RouteGroupBuilder MapNymBrokerDashboard(this IEndpointRouteBuilder endpoints, string pattern = "/nymbroker")
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrWhiteSpace(pattern);
        if (endpoints.ServiceProvider.GetService<NymBrokerDashboardOptions>() is null)
            throw new InvalidOperationException("Call services.AddNymBrokerDashboard() before MapNymBrokerDashboard().");

        var group = endpoints.MapGroup(pattern);
        group.ExcludeFromDescription();
        group.AddEndpointFilter(DashboardAccess.FilterAsync);
        DashboardEndpoints.Map(group);
        return group;
    }
}
