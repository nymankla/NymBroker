using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Endpoint.Http;

namespace NymBroker.Tests;

/// <summary>
/// Tests for <see cref="HttpEndPoint"/> (#77). All run without a server: a stub <see cref="HttpMessageHandler"/> records
/// the requests and returns scripted responses. The webhook sample covers a real round trip over Kestrel.
/// </summary>
public sealed class HttpEndPointTests
{
    private const string Url = "https://hooks.example.com/orders";
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static byte[] Envelope(Guid id, Guid correlation, string payloadJson = """{"orderId":"A-1","amount":12.5}""", string messageType = "orders.created")
        => Encoding.UTF8.GetBytes($$"""{"id":"{{id}}","correlationId":"{{correlation}}","messageType":"{{messageType}}","created":"2026-01-01T00:00:00Z","message":{{payloadJson}}}""");

    // =====================================================================
    // Settings
    // =====================================================================

    public static TheoryData<string, HttpSettings> InvalidSettings => new()
    {
        { "no url", new HttpSettings() },
        { "relative url", new HttpSettings { Url = "/hooks" } },
        { "ftp url", new HttpSettings { Url = "ftp://example.com/x" } },
        { "GET", new HttpSettings { Url = Url, Method = "GET" } },
        { "zero timeout", new HttpSettings { Url = Url, Timeout = TimeSpan.Zero } },
        { "negative retries", new HttpSettings { Url = Url, MaxRetryAttempts = -1 } },
        { "bad health url", new HttpSettings { Url = Url, HealthCheckUrl = "health" } },
        { "empty secret", new HttpSettings { Url = Url, SigningSecret = "" } },
    };

    [Theory]
    [MemberData(nameof(InvalidSettings))]
    public void Settings_Invalid_Throw(string _, HttpSettings settings)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => settings.Validate("Hook"));
        Assert.Contains("'Hook'", ex.Message);
    }

    [Fact]
    public void AddHttpEndPoint_InvalidSettings_ThrowsAtRegistration()
        => Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddNymBroker().AddHttpEndPoint("Hook", new HttpSettings()));

    // =====================================================================
    // Request shape
    // =====================================================================

    [Fact]
    public async Task Envelope_IsPostedUnchanged_WithIdempotencyAndMetadataHeaders()
    {
        var (ep, handler) = Create(new HttpSettings { Url = Url, Headers = { ["X-Api-Key"] = "secret-key" } });
        var id = Guid.NewGuid();
        var correlation = Guid.NewGuid();
        var envelope = Envelope(id, correlation);

        await ep.PostAsync(envelope, Ct);

        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal(Url, request.Uri.ToString());
        Assert.Equal(envelope, request.Body);
        Assert.Equal("application/json", request.ContentType);
        Assert.Equal(id.ToString(), request.Headers["Idempotency-Key"]);
        Assert.Equal(id.ToString(), request.Headers["X-NymBroker-Message-Id"]);
        Assert.Equal("orders.created", request.Headers["X-NymBroker-Message-Type"]);
        Assert.Equal(correlation.ToString(), request.Headers["X-NymBroker-Correlation-Id"]);
        Assert.Equal("secret-key", request.Headers["X-Api-Key"]);
    }

    [Fact]
    public async Task PayloadFormat_SendsOnlyTheMessagePart()
    {
        var (ep, handler) = Create(new HttpSettings { Url = Url, BodyFormat = HttpBodyFormat.Payload, Method = "put" });

        await ep.PostAsync(Envelope(Guid.NewGuid(), Guid.NewGuid(), """{"orderId":"A-1","lines":[1,2]}"""), Ct);

        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Put, request.Method);
        Assert.Equal("""{"orderId":"A-1","lines":[1,2]}""", Encoding.UTF8.GetString(request.Body));
        Assert.True(request.Headers.ContainsKey("Idempotency-Key"));   // metadata still travels in headers
    }

    [Theory]
    [InlineData(HttpBodyFormat.Envelope)]
    [InlineData(HttpBodyFormat.Payload)]
    public async Task RawBytes_AreSentUnchanged_WithoutEnvelopeHeaders(HttpBodyFormat format)
    {
        var (ep, handler) = Create(new HttpSettings { Url = Url, BodyFormat = format });
        byte[] raw = [0x00, 0xFF, 0x10];

        await ep.PostAsync(raw, Ct);

        var request = Assert.Single(handler.Requests);
        Assert.Equal(raw, request.Body);
        Assert.Equal("application/octet-stream", request.ContentType);
        Assert.False(request.Headers.ContainsKey("Idempotency-Key"));
    }

    [Fact]
    public async Task Signature_IsHmacSha256_OverTimestampAndBody()
    {
        const string secret = "whsec_test";
        var now = new DateTimeOffset(2026, 10, 12, 12, 0, 0, TimeSpan.Zero);
        var (ep, handler) = Create(new HttpSettings { Url = Url, SigningSecret = secret }, new FixedTime(now));
        var envelope = Envelope(Guid.NewGuid(), Guid.NewGuid());

        await ep.PostAsync(envelope, Ct);

        var signature = Assert.Single(handler.Requests).Headers["X-NymBroker-Signature"];
        var t = now.ToUnixTimeSeconds();
        var expected = Convert.ToHexStringLower(HMACSHA256.HashData(Encoding.UTF8.GetBytes(secret), Encoding.UTF8.GetBytes($"{t}.").Concat(envelope).ToArray()));
        Assert.Equal($"t={t},v1={expected}", signature);
    }

    [Fact]
    public async Task NoSigningSecret_NoSignatureHeader()
    {
        var (ep, handler) = Create(new HttpSettings { Url = Url });
        await ep.PostAsync(Envelope(Guid.NewGuid(), Guid.NewGuid()), Ct);
        Assert.False(Assert.Single(handler.Requests).Headers.ContainsKey("X-NymBroker-Signature"));
    }

    // =====================================================================
    // Responses, retries, errors
    // =====================================================================

    [Fact]
    public async Task ServerError_IsRetried_ThenSucceeds()
    {
        var logger = new CapturingLogger<HttpEndPoint>();
        var (ep, handler) = Create(new HttpSettings { Url = Url, RetryDelay = TimeSpan.Zero, Headers = { ["X-Api-Key"] = "never-logged" } }, logger: logger,
            responses: [HttpStatusCode.ServiceUnavailable, HttpStatusCode.BadGateway, HttpStatusCode.Accepted]);

        await ep.PostAsync(Envelope(Guid.NewGuid(), Guid.NewGuid()), Ct);

        Assert.Equal(3, handler.Requests.Count);
        Assert.Equal(2, logger.Entries.Count(e => e.Level == LogLevel.Warning && e.Message.Contains("retrying")));
        Assert.DoesNotContain(logger.Entries, e => e.Message.Contains("never-logged"));
    }

    [Fact]
    public async Task TransientFailures_ExhaustTheRetries_ThenThrowWithTheStatus()
    {
        var (ep, handler) = Create(new HttpSettings { Url = Url, RetryDelay = TimeSpan.Zero, MaxRetryAttempts = 2 },
            responses: [HttpStatusCode.InternalServerError]);

        var ex = await Assert.ThrowsAsync<HttpRequestException>(() => ep.PostAsync(Envelope(Guid.NewGuid(), Guid.NewGuid()), Ct));

        Assert.Equal(HttpStatusCode.InternalServerError, ex.StatusCode);
        Assert.Equal(3, handler.Requests.Count);   // first attempt + 2 retries
    }

    [Theory]
    [InlineData(HttpStatusCode.BadRequest)]
    [InlineData(HttpStatusCode.Unauthorized)]
    [InlineData(HttpStatusCode.NotFound)]
    [InlineData(HttpStatusCode.UnprocessableEntity)]
    public async Task PermanentClientError_IsNotRetried_AndThrows(HttpStatusCode status)
    {
        var (ep, handler) = Create(new HttpSettings { Url = Url, RetryDelay = TimeSpan.Zero }, responses: [status]);

        var ex = await Assert.ThrowsAsync<HttpRequestException>(() => ep.PostAsync(Envelope(Guid.NewGuid(), Guid.NewGuid()), Ct));

        Assert.Equal(status, ex.StatusCode);
        Assert.Single(handler.Requests);
    }

    [Theory]
    [InlineData(HttpStatusCode.RequestTimeout)]
    [InlineData(HttpStatusCode.TooManyRequests)]
    public async Task RequestTimeoutAndTooManyRequests_AreRetried(HttpStatusCode status)
    {
        var (ep, handler) = Create(new HttpSettings { Url = Url, RetryDelay = TimeSpan.Zero }, responses: [status, HttpStatusCode.OK]);

        await ep.PostAsync(Envelope(Guid.NewGuid(), Guid.NewGuid()), Ct);

        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task RetryAfter_IsHonoured()
    {
        var (ep, handler) = Create(new HttpSettings { Url = Url, RetryDelay = TimeSpan.Zero },
            responses: [HttpStatusCode.TooManyRequests, HttpStatusCode.OK], retryAfter: TimeSpan.FromSeconds(1));

        await ep.PostAsync(Envelope(Guid.NewGuid(), Guid.NewGuid()), Ct);

        Assert.Equal(2, handler.Requests.Count);
        Assert.True(handler.Requests[1].At - handler.Requests[0].At >= TimeSpan.FromMilliseconds(900),
            $"second attempt after {handler.Requests[1].At - handler.Requests[0].At}");
    }

    [Fact]
    public async Task NetworkError_IsRetried()
    {
        var (ep, handler) = Create(new HttpSettings { Url = Url, RetryDelay = TimeSpan.Zero }, throwFirst: new HttpRequestException("connection refused"));

        await ep.PostAsync(Envelope(Guid.NewGuid(), Guid.NewGuid()), Ct);

        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task Timeout_IsRetried_ThenThrowsTimeoutException()
    {
        var handler = new StubHandler { Hang = true };
        var client = new HttpClient(handler) { Timeout = TimeSpan.FromMilliseconds(100) };
        var ep = new HttpEndPoint("Hook", new HttpSettings { Url = Url, RetryDelay = TimeSpan.Zero, MaxRetryAttempts = 1 }, client,
            new CapturingLogger<HttpEndPoint>());

        await Assert.ThrowsAsync<TimeoutException>(() => ep.PostAsync(Envelope(Guid.NewGuid(), Guid.NewGuid()), Ct));
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task CallerCancellation_IsNotRetried()
    {
        var handler = new StubHandler { Hang = true };
        var ep = new HttpEndPoint("Hook", new HttpSettings { Url = Url, RetryDelay = TimeSpan.Zero }, new HttpClient(handler),
            new CapturingLogger<HttpEndPoint>());
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ep.PostAsync(Envelope(Guid.NewGuid(), Guid.NewGuid()), cts.Token));
        Assert.Single(handler.Requests);
    }

    [Fact]
    public async Task ZeroRetries_SendsOnce()
    {
        var (ep, handler) = Create(new HttpSettings { Url = Url, MaxRetryAttempts = 0 }, responses: [HttpStatusCode.ServiceUnavailable]);
        await Assert.ThrowsAsync<HttpRequestException>(() => ep.PostAsync(Envelope(Guid.NewGuid(), Guid.NewGuid()), Ct));
        Assert.Single(handler.Requests);
    }

    // =====================================================================
    // Health check
    // =====================================================================

    [Fact]
    public void HealthCheck_WithoutProbeUrl_IsHealthy_AndCallsNothing()
    {
        var (ep, handler) = Create(new HttpSettings { Url = Url });
        Assert.True(ep.HealthCheck().IsHealthy);
        Assert.Empty(handler.Requests);
    }

    [Theory]
    [InlineData(HttpStatusCode.OK, true)]
    [InlineData(HttpStatusCode.ServiceUnavailable, false)]
    public void HealthCheck_ProbesTheHealthUrl(HttpStatusCode status, bool healthy)
    {
        var logger = new CapturingLogger<HttpEndPoint>();
        var (ep, handler) = Create(new HttpSettings { Url = Url, HealthCheckUrl = "https://hooks.example.com/health" }, logger: logger, responses: [status]);

        var result = ep.HealthCheck();

        Assert.Equal(healthy, result.IsHealthy);
        Assert.Equal(HttpMethod.Get, Assert.Single(handler.Requests).Method);
        Assert.Equal("https://hooks.example.com/health", handler.Requests[0].Uri.ToString());
        if (!healthy) Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error);
    }

    // =====================================================================
    // Registration and the broker
    // =====================================================================

    [Fact]
    public async Task AddHttpEndPoint_UsesANamedHttpClient_WithTheTimeout_AndCustomHandlers()
    {
        var handler = new StubHandler();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .AddHttpEndPoint("Hook", new HttpSettings { Url = Url, Timeout = TimeSpan.FromSeconds(7) },
                client => client.ConfigurePrimaryHttpMessageHandler(() => handler))
            .Build();
        await using var sp = services.BuildServiceProvider();

        var ep = Assert.IsType<HttpEndPoint>(sp.GetRequiredKeyedService<IEndPoint>("Hook"));
        await ep.PostAsync(Envelope(Guid.NewGuid(), Guid.NewGuid()), Ct);

        Assert.Single(handler.Requests);
        Assert.Equal(EndpointMode.WriteOnly, ep.Mode);
        Assert.Equal(TimeSpan.FromSeconds(7), sp.GetRequiredService<IHttpClientFactory>().CreateClient("NymBroker.Http.Hook").Timeout);
    }

    [Fact]
    public async Task WithHttp_RegistersConfiguredEndpoint_IncludingEnumAndHeaders()
    {
        const string json = """
            {
              "NymBroker": {
                "Endpoints": [
                  { "name": "Hook1", "type": "http",
                    "config": { "url": "https://hooks.example.com/x", "method": "PUT", "bodyFormat": "Payload",
                                "timeout": "00:00:05", "headers": { "X-Api-Key": "k" } } }
                ]
              }
            }
            """;
        var path = Path.GetTempFileName();
        try
        {
            File.WriteAllText(path, json);
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddNymBroker().LoadConfiguration(path).WithHttp().Build();
            await using var sp = services.BuildServiceProvider();

            Assert.IsType<HttpEndPoint>(sp.GetRequiredKeyedService<IEndPoint>("Hook1"));
            Assert.Equal(TimeSpan.FromSeconds(5), sp.GetRequiredService<IHttpClientFactory>().CreateClient("NymBroker.Http.Hook1").Timeout);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [MessageName("tests.http.order")]
    public sealed class HttpOrder { public string OrderId { get; set; } = ""; }

    [Fact]
    public async Task ThroughBroker_ARouteToTheHttpEndpoint_DeliversTheEnvelope_AndAFailureIsRetriedBySource()
    {
        var handler = new StubHandler { Responses = [HttpStatusCode.BadRequest, HttpStatusCode.OK] };
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .AddMemoryEndPoint("In")
            .AddHttpEndPoint("Hook", new HttpSettings { Url = Url, RetryDelay = TimeSpan.Zero },
                client => client.ConfigurePrimaryHttpMessageHandler(() => handler))
            .Build();
        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<NymBrokerImpl>();
        broker.Route<HttpOrder>().To("Hook").Build();

        // A route destination that throws (here: 400) makes ProcessAsync return Retry — the source redelivers.
        var first = await broker.ProcessAsync(Envelope(Guid.NewGuid(), Guid.NewGuid(), """{"orderId":"R-1"}""", "tests.http.order"), "In", Ct);
        var second = await broker.ProcessAsync(Envelope(Guid.NewGuid(), Guid.NewGuid(), """{"orderId":"R-2"}""", "tests.http.order"), "In", Ct);

        Assert.Equal(ProcessOutcome.Retry, first.Outcome);
        Assert.Equal(ProcessOutcome.Completed, second.Outcome);
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task BrokerHealth_WithFailingProbe_IsUnhealthy()
    {
        var handler = new StubHandler { Responses = [HttpStatusCode.ServiceUnavailable] };
        var report = await BrokerHealthTestHelper.CheckAsync(b => b
            .AddHttpEndPoint("Hook", new HttpSettings { Url = Url, HealthCheckUrl = "https://hooks.example.com/health" },
                client => client.ConfigurePrimaryHttpMessageHandler(() => handler)));

        Assert.Equal(BrokerHealthStatus.Unhealthy, report.Status);
        Assert.Contains("Hook", report.Message);
    }

    // =====================================================================
    // helpers
    // =====================================================================

    private static (HttpEndPoint Endpoint, StubHandler Handler) Create(HttpSettings settings, TimeProvider? time = null,
        CapturingLogger<HttpEndPoint>? logger = null, HttpStatusCode[]? responses = null, TimeSpan? retryAfter = null, Exception? throwFirst = null)
    {
        var handler = new StubHandler { Responses = responses ?? [HttpStatusCode.OK], RetryAfter = retryAfter, ThrowFirst = throwFirst };
        var ep = new HttpEndPoint("Hook", settings, new HttpClient(handler), logger ?? new CapturingLogger<HttpEndPoint>(), time ?? TimeProvider.System);
        return (ep, handler);
    }

    private sealed record RecordedRequest(HttpMethod Method, Uri Uri, byte[] Body, string? ContentType, Dictionary<string, string> Headers, DateTime At);

    /// <summary>Records each request and answers with the next scripted status (the last one repeats).</summary>
    private sealed class StubHandler : HttpMessageHandler
    {
        private int _calls;
        public List<RecordedRequest> Requests { get; } = [];
        public HttpStatusCode[] Responses { get; init; } = [HttpStatusCode.OK];
        public TimeSpan? RetryAfter { get; init; }
        public Exception? ThrowFirst { get; init; }
        public bool Hang { get; init; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var body = request.Content is null ? [] : await request.Content.ReadAsByteArrayAsync(ct);
            var headers = request.Headers.Concat(request.Content?.Headers ?? Enumerable.Empty<KeyValuePair<string, IEnumerable<string>>>())
                .ToDictionary(h => h.Key, h => string.Join(",", h.Value), StringComparer.OrdinalIgnoreCase);
            lock (Requests)
                Requests.Add(new RecordedRequest(request.Method, request.RequestUri!, body, request.Content?.Headers.ContentType?.MediaType, headers, DateTime.UtcNow));

            var call = Interlocked.Increment(ref _calls) - 1;
            if (Hang) await Task.Delay(Timeout.Infinite, ct);
            if (call == 0 && ThrowFirst is not null) throw ThrowFirst;

            var response = new HttpResponseMessage(Responses[Math.Min(call, Responses.Length - 1)]);
            if (RetryAfter is { } wait) response.Headers.RetryAfter = new RetryConditionHeaderValue(wait);
            return response;
        }
    }

    private sealed class FixedTime(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
