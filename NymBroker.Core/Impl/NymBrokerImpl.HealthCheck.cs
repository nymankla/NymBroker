using System.Diagnostics;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Impl;

public sealed partial class NymBrokerImpl
{
    internal const string BrokerNotRunningMessage = "Broker is not running";

    private BrokerHealthCheckOptions _healthCheckOptions = new();

    /// <summary>Sets the options used by <see cref="CheckHealthAsync"/> (configuration time).</summary>
    public void ConfigureHealthCheck(BrokerHealthCheckOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _healthCheckOptions = options;
    }

    public async Task<BrokerHealthReport> CheckHealthAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        var options = _healthCheckOptions;
        var total = Stopwatch.StartNew();
        var started = Volatile.Read(ref _started);
        var endpoints = _endpoints.OrderBy(kvp => kvp.Key, StringComparer.OrdinalIgnoreCase).ToList();

        // HealthCheck() is synchronous and may block (database / Service Bus endpoints contact their server),
        // so every endpoint runs on the thread pool and the total time is about the slowest one.
        var checks = new Task<(EndpointHealth Health, string? FailureReason)>[endpoints.Count];
        var modes = new EndpointMode[endpoints.Count];
        for (var i = 0; i < endpoints.Count; i++)
        {
            var (name, endpoint) = (endpoints[i].Key, endpoints[i].Value);
            modes[i] = SafeMode(name, endpoint);
            var mode = modes[i];
            var critical = options.IsCritical(name);
            checks[i] = Task.Run(() => CheckEndpoint(name, endpoint, mode, critical), CancellationToken.None);
        }

        if (checks.Length > 0)
        {
            using var delayCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var all = Task.WhenAll(checks);
            var delay = Task.Delay(options.Timeout, delayCts.Token);
            await Task.WhenAny(all, delay).ConfigureAwait(false);
            await delayCts.CancelAsync().ConfigureAwait(false);   // stop the timer when the checks won
            ct.ThrowIfCancellationRequested();
        }

        var results = new EndpointHealth[checks.Length];
        var failureReasons = new string?[checks.Length];
        for (var i = 0; i < checks.Length; i++)
        {
            if (checks[i].IsCompletedSuccessfully)
            {
                (results[i], failureReasons[i]) = checks[i].Result;
                continue;
            }

            failureReasons[i] = HealthFailureTimeout;

            // Still running: report it and leave the task to finish in the background (CheckEndpoint never throws).
            var name = endpoints[i].Key;
            var message = $"Health check timed out after {options.Timeout}";
            _logger.LogWarning("Health check of endpoint '{Name}' did not answer within {Timeout}", name, options.Timeout);
            results[i] = new EndpointHealth(name, BrokerHealthStatus.Unhealthy, message, modes[i], total.Elapsed,
                options.IsCritical(name));
        }

        var (status, summary) = Aggregate(started, results);
        var report = new BrokerHealthReport(status, started, results, total.Elapsed, summary);
        RecordHealthMetrics(report, failureReasons);

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Broker health check: {Status} in {Duration} ms ({Summary})",
                status.ToString(), total.Elapsed.TotalMilliseconds, summary ?? "all healthy");

        return report;
    }

    // Values of the "reason" tag on nymbroker.health.endpoint.failures.
    internal const string HealthFailureUnhealthy = "unhealthy";
    internal const string HealthFailureTimeout = "timeout";
    internal const string HealthFailureError = "error";

    /// <summary>Checks one endpoint; never throws. The failure reason is null when the endpoint is healthy.</summary>
    private (EndpointHealth Health, string? FailureReason) CheckEndpoint(string name, IEndPoint endpoint, EndpointMode mode, bool critical)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            var result = endpoint.HealthCheck();
            if (result is null)
                return (new EndpointHealth(name, BrokerHealthStatus.Unhealthy, "Health check returned no result", mode, sw.Elapsed, critical),
                    HealthFailureError);

            return result.IsHealthy
                ? (new EndpointHealth(name, BrokerHealthStatus.Healthy, result.Message, mode, sw.Elapsed, critical), null)
                : (new EndpointHealth(name, BrokerHealthStatus.Unhealthy, result.Message, mode, sw.Elapsed, critical), HealthFailureUnhealthy);
        }
        catch (Exception ex)
        {
            // IEndPoint.HealthCheck() should not throw; report it instead of failing the whole check.
            _logger.LogWarning(ex, "Health check of endpoint '{Name}' threw", name);
            return (new EndpointHealth(name, BrokerHealthStatus.Unhealthy, ex.Message, mode, sw.Elapsed, critical), HealthFailureError);
        }
    }

    /// <summary>Records the health metrics (counters for alerting on rates, gauges for the latest state).</summary>
    private static void RecordHealthMetrics(BrokerHealthReport report, IReadOnlyList<string?> failureReasons)
    {
        NymBrokerDiagnostics.HealthChecks.Add(1, new KeyValuePair<string, object?>("status", StatusTag(report.Status)));
        NymBrokerDiagnostics.HealthStatus.Record((int)report.Status);

        for (var i = 0; i < report.Endpoints.Count; i++)
        {
            var endpoint = report.Endpoints[i];
            var endpointTag = new KeyValuePair<string, object?>("endpoint", endpoint.Name);
            var criticalTag = new KeyValuePair<string, object?>("critical", endpoint.IsCritical);

            NymBrokerDiagnostics.HealthEndpointHealthy.Record(endpoint.IsHealthy ? 1 : 0, endpointTag, criticalTag);

            if (!endpoint.IsHealthy)
                NymBrokerDiagnostics.HealthEndpointFailures.Add(1, endpointTag, criticalTag,
                    new KeyValuePair<string, object?>("reason", failureReasons[i] ?? HealthFailureUnhealthy));
        }
    }

    private static string StatusTag(BrokerHealthStatus status) => status switch
    {
        BrokerHealthStatus.Healthy => "healthy",
        BrokerHealthStatus.Degraded => "degraded",
        _ => "unhealthy"
    };

    private EndpointMode SafeMode(string name, IEndPoint endpoint)
    {
        try
        {
            return endpoint.Mode;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Reading the mode of endpoint '{Name}' threw", name);
            return EndpointMode.ReadWrite;
        }
    }

    internal static (BrokerHealthStatus Status, string? Message) Aggregate(bool brokerStarted, IReadOnlyList<EndpointHealth> endpoints)
    {
        var failedCritical = endpoints.Where(e => !e.IsHealthy && e.IsCritical).Select(e => e.Name).ToList();
        var failedNonCritical = endpoints.Where(e => !e.IsHealthy && !e.IsCritical).Select(e => e.Name).ToList();

        var parts = new List<string>();
        if (!brokerStarted) parts.Add(BrokerNotRunningMessage);
        if (failedCritical.Count > 0) parts.Add($"Unhealthy endpoints: {string.Join(", ", failedCritical)}");
        if (failedNonCritical.Count > 0) parts.Add($"Unhealthy non-critical endpoints: {string.Join(", ", failedNonCritical)}");

        var status = !brokerStarted || failedCritical.Count > 0 ? BrokerHealthStatus.Unhealthy
            : failedNonCritical.Count > 0 ? BrokerHealthStatus.Degraded
            : BrokerHealthStatus.Healthy;

        return (status, parts.Count == 0 ? null : string.Join("; ", parts));
    }
}
