using System.Diagnostics;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Impl;

internal sealed partial class NymBrokerImpl
{
    internal const string BrokerNotRunningMessage = "Broker is not running";

    private BrokerHealthCheckOptions _healthCheckOptions = new();

    /// <summary>Samples the queue endpoints (<c>MonitorQueues</c>, #133); null when monitoring is off.</summary>
    internal Endpoint.Queue.QueueMonitor? QueueMonitor { get; private set; }

    /// <summary>Sets the options used by <see cref="CheckHealthAsync"/> (configuration time).</summary>
    public void ConfigureHealthCheck(BrokerHealthCheckOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _healthCheckOptions = options;
    }

    /// <summary>Creates the queue monitor; its hosted service runs the samples (configuration time).</summary>
    public void EnableQueueMonitoring(Endpoint.Queue.QueueMonitoringOptions options, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        QueueMonitor = new Endpoint.Queue.QueueMonitor(options, () => _endpoints, () => _deadLetterEndpoint, _logger, timeProvider);
    }

    public async Task<BrokerHealthReport> CheckHealthAsync(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        var options = _healthCheckOptions;
        var total = Stopwatch.StartNew();
        var started = Volatile.Read(ref _started);

        var checks = StartEndpointChecks(options, ct);
        await WaitForChecksAsync(checks, options.Timeout, ct).ConfigureAwait(false);

        var results = new EndpointHealth[checks.Count];
        var failureReasons = new string?[checks.Count];
        for (var i = 0; i < checks.Count; i++)
            (results[i], failureReasons[i]) = ResultOrTimeout(checks[i], options, total.Elapsed);
        ApplyDeadLetterThresholds(results);

        var (status, summary) = Aggregate(started, results);
        var report = new BrokerHealthReport(status, started, results, total.Elapsed, summary);
        RecordHealthMetrics(report, failureReasons);

        if (_logger.IsEnabled(LogLevel.Debug))
            _logger.LogDebug("Broker health check: {Status} in {Duration} ms ({Summary})",
                status.ToString(), total.Elapsed.TotalMilliseconds, summary ?? "all healthy");

        return report;
    }

    /// <summary>One endpoint's running check, with what is needed to report it if it doesn't finish in time.</summary>
    private sealed record EndpointCheck(string Name, EndpointMode Mode, Task<(EndpointHealth Health, string? FailureReason)> Task);

    /// <summary>
    /// Starts every endpoint's check in parallel, ordered by name, so the total time is about the slowest one. Task.Run
    /// keeps an endpoint that only has the synchronous HealthCheck() (the default HealthCheckAsync) from blocking the caller.
    /// </summary>
    private List<EndpointCheck> StartEndpointChecks(BrokerHealthCheckOptions options, CancellationToken ct)
        => _endpoints.OrderBy(kvp => kvp.Key, StringComparer.OrdinalIgnoreCase)
            .Select(kvp =>
            {
                var (name, endpoint) = (kvp.Key, kvp.Value);
                var mode = SafeMode(name, endpoint);
                var critical = options.IsCritical(name);
                return new EndpointCheck(name, mode,
                    Task.Run(() => CheckEndpointAsync(name, endpoint, mode, critical, ct), CancellationToken.None));
            })
            .ToList();

    /// <summary>Waits until every check has finished or the timeout has passed; only the caller's cancellation throws.</summary>
    private static async Task WaitForChecksAsync(List<EndpointCheck> checks, TimeSpan timeout, CancellationToken ct)
    {
        if (checks.Count == 0) return;

        using var delayCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var all = Task.WhenAll(checks.Select(c => c.Task));
        var delay = Task.Delay(timeout, delayCts.Token);
        await Task.WhenAny(all, delay).ConfigureAwait(false);
        await delayCts.CancelAsync().ConfigureAwait(false);   // stop the timer when the checks won
        ct.ThrowIfCancellationRequested();
    }

    /// <summary>The check's result, or Unhealthy "timed out" if it is still running (it finishes in the background; CheckEndpointAsync never throws).</summary>
    private (EndpointHealth Health, string? FailureReason) ResultOrTimeout(EndpointCheck check, BrokerHealthCheckOptions options, TimeSpan elapsed)
    {
        if (check.Task.IsCompletedSuccessfully)
            return check.Task.Result;

        _logger.LogWarning("Health check of endpoint '{Name}' did not answer within {Timeout}", check.Name, options.Timeout);
        return (new EndpointHealth(check.Name, BrokerHealthStatus.Unhealthy, $"Health check timed out after {options.Timeout}",
            check.Mode, elapsed, options.IsCritical(check.Name)), HealthFailureTimeout);
    }

    /// <summary>
    /// A reachable endpoint over its dead-letter threshold becomes Degraded: monitoring should see the backlog, but a
    /// restart can't fix it, so it never makes the broker Unhealthy.
    /// </summary>
    private void ApplyDeadLetterThresholds(EndpointHealth[] results)
    {
        if (QueueMonitor is not { } monitor) return;
        for (var i = 0; i < results.Length; i++)
        {
            if (results[i].IsHealthy && monitor.DeadLetterBreach(results[i].Name) is { } breach)
                results[i] = results[i] with { Status = BrokerHealthStatus.Degraded, Message = breach };
        }
    }

    public Endpoint.Queue.IQueueInspector? GetQueueInspector(string endpointName)
        => RegisteredEndpoint(endpointName) as Endpoint.Queue.IQueueInspector;

    public Endpoint.Queue.IQueueReplayer? GetQueueReplayer(string endpointName)
        => RegisteredEndpoint(endpointName) is Endpoint.Queue.IQueueReplayer replayer
            ? new Endpoint.Queue.AuditedQueueReplayer(endpointName, replayer, _logger)
            : null;

    public Endpoint.Queue.IQueueFailedRemover? GetQueueFailedRemover(string endpointName)
        => RegisteredEndpoint(endpointName) is Endpoint.Queue.IQueueFailedRemover remover
            ? new Endpoint.Queue.AuditedQueueFailedRemover(endpointName, remover, _logger)
            : null;

    private IEndPoint RegisteredEndpoint(string endpointName)
    {
        ArgumentException.ThrowIfNullOrEmpty(endpointName);
        return _endpoints.TryGetValue(endpointName, out var endpoint)
            ? endpoint
            : throw new InvalidOperationException($"No endpoint registered with name '{endpointName}'.");
    }

    // Values of the "reason" tag on nymbroker.health.endpoint.failures.
    internal const string HealthFailureUnhealthy = "unhealthy";
    internal const string HealthFailureTimeout = "timeout";
    internal const string HealthFailureError = "error";

    /// <summary>Checks one endpoint; never throws. The failure reason is null when the endpoint is healthy.</summary>
    private async Task<(EndpointHealth Health, string? FailureReason)> CheckEndpointAsync(string name, IEndPoint endpoint,
        EndpointMode mode, bool critical, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            var result = await endpoint.HealthCheckAsync(ct).ConfigureAwait(false);
            if (result is null)
                return (new EndpointHealth(name, BrokerHealthStatus.Unhealthy, "Health check returned no result", mode, sw.Elapsed, critical),
                    HealthFailureError);

            return result.IsHealthy
                ? (new EndpointHealth(name, BrokerHealthStatus.Healthy, result.Message, mode, sw.Elapsed, critical), null)
                : (new EndpointHealth(name, BrokerHealthStatus.Unhealthy, result.Message, mode, sw.Elapsed, critical), HealthFailureUnhealthy);
        }
        catch (Exception ex)
        {
            // A health check should not throw; report it instead of failing the whole check (caller cancellation is
            // re-checked by CheckHealthAsync).
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
            // A Degraded endpoint (over its dead-letter threshold) is reachable: it counts as healthy here.
            var reachable = endpoint.Status != BrokerHealthStatus.Unhealthy;

            NymBrokerDiagnostics.HealthEndpointHealthy.Record(reachable ? 1 : 0, endpointTag, criticalTag);

            if (!reachable)
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
        var failedCritical = Names(endpoints, e => e.Status == BrokerHealthStatus.Unhealthy && e.IsCritical);
        var failedNonCritical = Names(endpoints, e => e.Status == BrokerHealthStatus.Unhealthy && !e.IsCritical);
        var degraded = Names(endpoints, e => e.Status == BrokerHealthStatus.Degraded);

        var parts = new[]
        {
            brokerStarted ? null : BrokerNotRunningMessage,
            Listed("Unhealthy endpoints", failedCritical),
            Listed("Unhealthy non-critical endpoints", failedNonCritical),
            Listed("Endpoints over their dead-letter threshold", degraded)
        }.OfType<string>().ToList();

        var status = !brokerStarted || failedCritical.Count > 0 ? BrokerHealthStatus.Unhealthy
            : failedNonCritical.Count + degraded.Count > 0 ? BrokerHealthStatus.Degraded
            : BrokerHealthStatus.Healthy;

        return (status, parts.Count == 0 ? null : string.Join("; ", parts));
    }

    private static List<string> Names(IReadOnlyList<EndpointHealth> endpoints, Func<EndpointHealth, bool> predicate)
        => endpoints.Where(predicate).Select(e => e.Name).ToList();

    private static string? Listed(string label, List<string> names)
        => names.Count == 0 ? null : $"{label}: {string.Join(", ", names)}";
}
