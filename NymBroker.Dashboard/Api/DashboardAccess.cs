using System.Net;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace NymBroker.Dashboard.Api;

/// <summary>
/// Runs before every dashboard request: refuses remote callers unless the dashboard has an authorization policy (or
/// <see cref="NymBrokerDashboardOptions.AllowAnonymousRemoteAccess"/>), and adds the security headers.
/// </summary>
internal static class DashboardAccess
{
    /// <summary>
    /// Everything comes from the dashboard's own origin, so the policy can forbid all else: no inline script or style,
    /// no framing, no form posts.
    /// </summary>
    internal const string ContentSecurityPolicy =
        "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; " +
        "base-uri 'none'; form-action 'none'; frame-ancestors 'none'";

    private static int _warnedAboutMissingPolicy;

    public static async ValueTask<object?> FilterAsync(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
    {
        var http = context.HttpContext;
        var options = http.RequestServices.GetRequiredService<NymBrokerDashboardOptions>();
        if (!IsAllowed(http, options))
            return Results.Text("The NymBroker dashboard answers only local requests unless it has an authorization policy. " +
                "Add .RequireAuthorization(...) to MapNymBrokerDashboard.", statusCode: StatusCodes.Status403Forbidden);

        AddSecurityHeaders(http.Response);
        return await next(context);
    }

    internal static bool IsAllowed(HttpContext http, NymBrokerDashboardOptions options)
    {
        if (HasAuthorizationPolicy(http) || options.AllowAnonymousRemoteAccess) return true;
        WarnOnce(http);
        return IsLocal(http.Request, http.Connection.RemoteIpAddress);
    }

    private static bool HasAuthorizationPolicy(HttpContext http)
        => http.GetEndpoint()?.Metadata.GetMetadata<IAuthorizeData>() is not null;

    /// <summary>
    /// A loopback caller, or none (an in-process test server). A request with forwarding headers came through a proxy,
    /// which may itself run on this machine, so it counts as remote.
    /// </summary>
    internal static bool IsLocal(HttpRequest request, IPAddress? remote)
    {
        if (request.Headers.ContainsKey("X-Forwarded-For") || request.Headers.ContainsKey("Forwarded")) return false;
        if (remote is null) return true;
        if (remote.IsIPv4MappedToIPv6) remote = remote.MapToIPv4();
        return IPAddress.IsLoopback(remote);
    }

    private static void WarnOnce(HttpContext http)
    {
        if (Interlocked.Exchange(ref _warnedAboutMissingPolicy, 1) == 1) return;
        http.RequestServices.GetRequiredService<ILoggerFactory>().CreateLogger(typeof(DashboardAccess).FullName!)
            .LogWarning("The NymBroker dashboard has no authorization policy, so it answers only requests from this machine. " +
                "Add .RequireAuthorization(...) to MapNymBrokerDashboard to open it to others.");
    }

    private static void AddSecurityHeaders(HttpResponse response)
    {
        var headers = response.Headers;
        headers.ContentSecurityPolicy = ContentSecurityPolicy;
        headers.XContentTypeOptions = "nosniff";
        headers.XFrameOptions = "DENY";
        headers["Referrer-Policy"] = "no-referrer";
        headers["Cross-Origin-Opener-Policy"] = "same-origin";
    }
}
