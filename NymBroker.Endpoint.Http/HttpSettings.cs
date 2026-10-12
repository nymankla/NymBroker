using System.Text.Json.Serialization;

namespace NymBroker.Endpoint.Http;

/// <summary>What the HTTP request body contains.</summary>
[JsonConverter(typeof(JsonStringEnumConverter<HttpBodyFormat>))]
public enum HttpBodyFormat
{
    /// <summary>The NymBroker envelope as-is (<c>application/json</c>) — for another NymBroker instance or a receiver that wants the metadata.</summary>
    Envelope,

    /// <summary>Only the envelope's <c>message</c> part — for a third-party API that expects its own JSON.</summary>
    Payload
}

public sealed class HttpSettings
{
    /// <summary>Absolute <c>http</c> or <c>https</c> URL every message is sent to.</summary>
    public string Url { get; set; } = "";

    /// <summary>HTTP method: <c>POST</c> (default), <c>PUT</c> or <c>PATCH</c>.</summary>
    public string Method { get; set; } = "POST";

    /// <summary>Extra request headers, e.g. an API key. Values are never logged.</summary>
    public Dictionary<string, string> Headers { get; set; } = [];

    /// <summary>Timeout per request attempt (sets the named <c>HttpClient</c>'s timeout).</summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);

    public HttpBodyFormat BodyFormat { get; set; } = HttpBodyFormat.Envelope;

    /// <summary>
    /// When set, each request carries <c>X-NymBroker-Signature: t=&lt;unix seconds&gt;,v1=&lt;hex HMAC-SHA256&gt;</c> over
    /// <c>"&lt;t&gt;." + body</c>, so the receiver can verify the sender and reject replays. Keep it in a secret store.
    /// </summary>
    public string? SigningSecret { get; set; }

    /// <summary>Retries after a transient failure (timeout, network error, 408, 429, 5xx). 0 disables retries.</summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>First retry delay; doubles per attempt (with jitter), at most 30 seconds. A 429/503 <c>Retry-After</c> header is honoured up to 60 seconds.</summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// URL probed with GET by the health check (any 2xx is healthy). Unset: the endpoint reports healthy without a probe
    /// — the target URL itself is never called just to check health.
    /// </summary>
    public string? HealthCheckUrl { get; set; }

    internal static readonly string[] AllowedMethods = ["POST", "PUT", "PATCH"];

    /// <summary>Throws <see cref="InvalidOperationException"/> describing the first invalid setting.</summary>
    internal void Validate(string endpointName)
    {
        void Fail(string message) => throw new InvalidOperationException($"HTTP endpoint '{endpointName}': {message}");

        if (!IsHttpUrl(Url))
            Fail($"Url '{Url}' must be an absolute http or https URL.");
        if (!AllowedMethods.Contains(Method.ToUpperInvariant()))
            Fail($"Method '{Method}' is not supported; use POST, PUT or PATCH.");
        if (Timeout <= TimeSpan.Zero)
            Fail("Timeout must be positive.");
        if (MaxRetryAttempts < 0)
            Fail("MaxRetryAttempts cannot be negative.");
        if (RetryDelay < TimeSpan.Zero)
            Fail("RetryDelay cannot be negative.");
        if (HealthCheckUrl is not null && !IsHttpUrl(HealthCheckUrl))
            Fail($"HealthCheckUrl '{HealthCheckUrl}' must be an absolute http or https URL.");
        if (SigningSecret is { Length: 0 })
            Fail("SigningSecret cannot be empty; leave it unset to disable signing.");
    }

    private static bool IsHttpUrl(string? url)
        => Uri.TryCreate(url, UriKind.Absolute, out var uri) && (uri.Scheme == Uri.UriSchemeHttp || uri.Scheme == Uri.UriSchemeHttps);
}
