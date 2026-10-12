using System.Net;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Resilience;

namespace NymBroker.Endpoint.Http;

/// <summary>
/// Send-only HTTP endpoint: each posted message becomes one request to <see cref="HttpSettings.Url"/>. A 2xx response
/// is success. Timeouts, network errors, 408, 429 and 5xx are retried (<see cref="HttpSettings.MaxRetryAttempts"/>,
/// exponential backoff, <c>Retry-After</c> honoured); after the last attempt, or for any other status, an
/// <see cref="HttpRequestException"/> with its <see cref="HttpRequestException.StatusCode"/> is thrown. A route or topic
/// destination that throws makes the broker return Retry, so the source transport redelivers the message and finally
/// dead-letters it — also for permanent failures such as 400, which are retried until the source's retry limit.
/// </summary>
public sealed class HttpEndPoint : IEndPoint
{
    /// <summary>Longest <c>Retry-After</c> the endpoint waits for; a longer one is capped.</summary>
    internal static readonly TimeSpan MaxRetryAfter = TimeSpan.FromSeconds(60);

    private readonly string _name;
    private readonly HttpSettings _settings;
    private readonly HttpClient _client;
    private readonly ILogger<HttpEndPoint> _logger;
    private readonly RetryPolicy? _retryPolicy;
    private readonly TimeProvider _timeProvider;

    /// <param name="client">
    /// The <see cref="HttpClient"/> to send with — normally the named client <c>AddHttpEndPoint</c> registers with
    /// <c>IHttpClientFactory</c>. Its <see cref="HttpClient.Timeout"/> limits each attempt.
    /// </param>
    public HttpEndPoint(string name, HttpSettings settings, HttpClient client, ILogger<HttpEndPoint> logger)
        : this(name, settings, client, logger, TimeProvider.System)
    {
    }

    internal HttpEndPoint(string name, HttpSettings settings, HttpClient client, ILogger<HttpEndPoint> logger, TimeProvider timeProvider)
    {
        settings.Validate(name);
        _name = name;
        _settings = settings;
        _client = client;
        _logger = logger;
        _timeProvider = timeProvider;

        if (settings.MaxRetryAttempts > 0)
        {
            _retryPolicy = new RetryPolicy(new RetryOptions
            {
                MaxRetryAttempts = settings.MaxRetryAttempts,
                Delay = settings.RetryDelay,
                MaxDelay = TimeSpan.FromSeconds(30),
                BackoffType = RetryBackoffType.Exponential,
                UseJitter = true,
                TimeProvider = timeProvider,
                ShouldHandle = IsTransient,
                OnRetry = args =>
                {
                    // Status and reason only: no body, no header values (they may hold secrets).
                    _logger.LogWarning("HTTP endpoint '{Name}': attempt {Attempt} failed ({Error}); retrying in {Delay}",
                        _name, args.AttemptNumber + 1, args.Exception.Message, args.Delay);
                    return ValueTask.CompletedTask;
                }
            });
        }
    }

    /// <summary>Always <see cref="EndpointMode.WriteOnly"/>: the endpoint only sends. Receive webhooks in your web app and pass them to <c>PostRawAsync</c>.</summary>
    public EndpointMode Mode => EndpointMode.WriteOnly;

    public async Task PostAsync(byte[] message, CancellationToken ct = default)
    {
        if (_retryPolicy is null)
        {
            await SendOnceAsync(message, null, ct);
            return;
        }

        TimeSpan? retryAfter = null;
        await _retryPolicy.ExecuteAsync(async token =>
        {
            if (retryAfter is { } wait)
                await Task.Delay(wait, _timeProvider, token);   // on top of the backoff: the server asked for at least this
            retryAfter = null;
            await SendOnceAsync(message, r => retryAfter = r, token);
        }, ct);
    }

    /// <summary>Sends one attempt. Returns normally on 2xx; otherwise throws (and reports a <c>Retry-After</c> through <paramref name="onRetryAfter"/>).</summary>
    private async Task SendOnceAsync(byte[] message, Action<TimeSpan?>? onRetryAfter, CancellationToken ct)
    {
        using var request = HttpRequestFactory.Create(_settings, message, _timeProvider.GetUtcNow());
        HttpResponseMessage response;
        try
        {
            response = await _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
        }
        catch (TaskCanceledException ex) when (!ct.IsCancellationRequested)
        {
            // HttpClient.Timeout surfaces as a cancellation; make it a retryable failure, not a shutdown.
            throw new TimeoutException($"HTTP endpoint '{_name}': no response within {_client.Timeout}.", ex);
        }

        using (response)
        {
            if (response.IsSuccessStatusCode)
                return;

            onRetryAfter?.Invoke(RetryAfter(response));
            throw new HttpRequestException(
                $"HTTP endpoint '{_name}': {_settings.Method.ToUpperInvariant()} returned {(int)response.StatusCode} {response.ReasonPhrase}.",
                null, response.StatusCode);
        }
    }

    public IHealthCheckResult HealthCheck()
    {
        if (_settings.HealthCheckUrl is not { } url)
            return HealthCheckResult.Healthy();

        return HealthCheckResult.FromProbe("HTTP", _name, _logger, async ct =>
        {
            using var response = await _client.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, ct);
            if (!response.IsSuccessStatusCode)
                throw new HttpRequestException($"Health check GET {url} returned {(int)response.StatusCode} {response.ReasonPhrase}.", null, response.StatusCode);
        });
    }

    /// <summary>Timeouts, network errors, 408 Request Timeout, 429 Too Many Requests and 5xx are worth another attempt.</summary>
    internal static bool IsTransient(Exception ex) => ex switch
    {
        TimeoutException => true,
        HttpRequestException { StatusCode: null } => true,   // connection refused, DNS, TLS, reset
        HttpRequestException { StatusCode: { } status } => status is HttpStatusCode.RequestTimeout or HttpStatusCode.TooManyRequests || (int)status >= 500,
        _ => false
    };

    private static TimeSpan? RetryAfter(HttpResponseMessage response)
    {
        if (response.StatusCode is not (HttpStatusCode.TooManyRequests or HttpStatusCode.ServiceUnavailable))
            return null;
        var header = response.Headers.RetryAfter;
        var wait = header?.Delta ?? (header?.Date is { } date ? date - DateTimeOffset.UtcNow : null);
        if (wait is not { } w || w <= TimeSpan.Zero) return null;
        return w > MaxRetryAfter ? MaxRetryAfter : w;
    }
}
