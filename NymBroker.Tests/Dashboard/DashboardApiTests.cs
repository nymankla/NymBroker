using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core;
using NymBroker.Core.DI;
using NymBroker.Core.Message;
using NymBroker.Dashboard;
using NymBroker.Endpoint.Sqlite;

namespace NymBroker.Tests.Dashboard;

/// <summary>The dashboard hosted on Kestrel on a loopback port, with a Memory endpoint and an in-memory SQLite queue.</summary>
public sealed class DashboardApiTests : IAsyncLifetime
{
    [MessageName("dashboard.order")]
    public sealed record DashboardOrder(string Customer, decimal Amount);

    private WebApplication _app = null!;
    private HttpClient _client = null!;

    private static async Task<(WebApplication App, HttpClient Client)> StartAsync(Action<NymBrokerDashboardOptions>? configure = null)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();
        builder.Services.AddNymBroker()
            .AddMemoryEndPoint("Inbox")
            .AddSqliteEndPoint("Queue", new SqliteSettings { ConnectionString = $"Data Source=dashboard-{Guid.NewGuid():N};Mode=Memory;Cache=Shared" })
            .Build();
        builder.Services.AddNymBrokerDashboard(o => { o.Title = "Test broker"; configure?.Invoke(o); });

        var app = builder.Build();
        app.MapNymBrokerDashboard();
        await app.StartAsync(TestContext.Current.CancellationToken);
        var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        var client = new HttpClient(new HttpClientHandler { AllowAutoRedirect = false }) { BaseAddress = new Uri(address + "/nymbroker/") };
        return (app, client);
    }

    public async ValueTask InitializeAsync() => (_app, _client) = await StartAsync();

    public async ValueTask DisposeAsync()
    {
        _client.Dispose();
        await _app.DisposeAsync();
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private async Task<JsonElement> GetJsonAsync(HttpClient client, string path)
    {
        using var response = await client.GetAsync(path, Ct);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        return JsonDocument.Parse(await response.Content.ReadAsStringAsync(Ct)).RootElement.Clone();
    }

    [Fact]
    public async Task ThePage_IsServedWithStrictSecurityHeaders_AndWithoutTheSlashItRedirects()
    {
        using var redirect = await _client.GetAsync("/nymbroker", Ct);
        Assert.Equal(HttpStatusCode.Redirect, redirect.StatusCode);
        Assert.Equal("/nymbroker/", redirect.Headers.Location!.OriginalString);

        using var page = await _client.GetAsync("", Ct);
        Assert.Equal(HttpStatusCode.OK, page.StatusCode);
        Assert.Equal("text/html", page.Content.Headers.ContentType!.MediaType);
        Assert.Contains("assets/app.js", await page.Content.ReadAsStringAsync(Ct));
        Assert.Contains("default-src 'none'", page.Headers.GetValues("Content-Security-Policy").Single());
        Assert.Equal("nosniff", page.Headers.GetValues("X-Content-Type-Options").Single());
        Assert.Equal("DENY", page.Headers.GetValues("X-Frame-Options").Single());
    }

    [Theory]
    [InlineData("assets/app.js", "text/javascript")]
    [InlineData("assets/app.css", "text/css")]
    [InlineData("assets/icon.svg", "image/svg+xml")]
    public async Task Assets_AreServed_AndRevalidatedByETag(string path, string contentType)
    {
        using var first = await _client.GetAsync(path, Ct);
        Assert.Equal(HttpStatusCode.OK, first.StatusCode);
        Assert.Equal(contentType, first.Content.Headers.ContentType!.MediaType);

        using var request = new HttpRequestMessage(HttpMethod.Get, path);
        request.Headers.IfNoneMatch.Add(first.Headers.ETag!);
        using var second = await _client.SendAsync(request, Ct);
        Assert.Equal(HttpStatusCode.NotModified, second.StatusCode);

        using var missing = await _client.GetAsync("assets/nope.js", Ct);
        Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);
    }

    [Fact]
    public async Task Overview_DescribesTheBroker_AndItsCurrentNumbers()
    {
        var overview = await GetJsonAsync(_client, "api/overview");

        Assert.Equal("Test broker", overview.GetProperty("title").GetString());
        Assert.Equal(900, overview.GetProperty("historySeconds").GetInt32());
        Assert.False(overview.GetProperty("showPayloads").GetBoolean());   // Production by default
        var endpoints = overview.GetProperty("broker").GetProperty("endpoints").EnumerateArray().ToDictionary(e => e.GetProperty("name").GetString()!);
        Assert.Equal("Sqlite", endpoints["Queue"].GetProperty("kind").GetString());
        Assert.True(endpoints["Queue"].GetProperty("inspectable").GetBoolean());
        Assert.Equal("ReadWrite", endpoints["Inbox"].GetProperty("mode").GetString());

        var live = overview.GetProperty("live");
        Assert.True(live.GetProperty("started").GetBoolean());
        var queue = live.GetProperty("endpoints").EnumerateArray().Single(e => e.GetProperty("name").GetString() == "Queue");
        Assert.Equal(0, queue.GetProperty("counts").GetProperty("pending").GetInt64());
    }

    [Fact]
    public async Task Messages_ListsAQueue_WithEnvelopeFields_AndHidesPayloadsByDefault()
    {
        var broker = _app.Services.GetRequiredService<INymBroker>();
        // Deferred far ahead, so the running broker leaves it in the queue.
        await broker.PostDeferredAsync("Queue", new DashboardOrder("Ada", 42), DateTimeOffset.UtcNow.AddDays(1), Ct);

        var page = await GetJsonAsync(_client, "api/endpoints/Queue/messages?state=deferred");

        var message = Assert.Single(page.GetProperty("messages").EnumerateArray());
        Assert.Equal("Deferred", message.GetProperty("state").GetString());
        Assert.Equal("dashboard.order", message.GetProperty("envelope").GetProperty("messageType").GetString());
        Assert.False(message.TryGetProperty("payload", out _));
        Assert.False(page.GetProperty("payloadsShown").GetBoolean());
    }

    [Fact]
    public async Task Messages_ShowsRedactedPayloads_WhenAllowed()
    {
        var (app, client) = await StartAsync(o =>
        {
            o.ShowPayloads = true;
            o.RedactPayload = (_, bytes) => Encoding.UTF8.GetBytes(Encoding.UTF8.GetString(bytes.Span).Replace("Ada", "***", StringComparison.Ordinal));
        });
        await using var _ = app;
        using var __ = client;
        await app.Services.GetRequiredService<INymBroker>().PostDeferredAsync("Queue", new DashboardOrder("Ada", 42), DateTimeOffset.UtcNow.AddDays(1), Ct);

        var page = await GetJsonAsync(client, "api/endpoints/Queue/messages?state=Deferred");

        var payload = Assert.Single(page.GetProperty("messages").EnumerateArray()).GetProperty("payload").GetString();
        Assert.Contains("\"customer\":\"***\"", payload);
        Assert.DoesNotContain("Ada", payload);
    }

    [Theory]
    [InlineData("api/endpoints/Nope/messages", HttpStatusCode.NotFound)]
    [InlineData("api/endpoints/Inbox/messages", HttpStatusCode.Conflict)]
    [InlineData("api/endpoints/Queue/messages?state=Completed", HttpStatusCode.BadRequest)]
    [InlineData("api/endpoints/Queue/messages?state=Lost", HttpStatusCode.BadRequest)]
    [InlineData("api/endpoints/Queue/messages?cursor=not-a-cursor", HttpStatusCode.BadRequest)]
    [InlineData("api/endpoints/Queue/messages?limit=many", HttpStatusCode.BadRequest)]
    [InlineData("api/endpoints/Queue/messages?state=Failed&failedSince=yesterday", HttpStatusCode.BadRequest)]
    public async Task Messages_ExplainsWhatItCannotList(string path, HttpStatusCode expected)
    {
        using var response = await _client.GetAsync(path, Ct);
        Assert.Equal(expected, response.StatusCode);
        var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync(Ct)).RootElement;
        Assert.False(string.IsNullOrEmpty(body.GetProperty("error").GetString()));
    }

    [Fact]
    public async Task Health_ChecksNow()
    {
        var health = await GetJsonAsync(_client, "api/health");
        Assert.Equal("Healthy", health.GetProperty("report").GetProperty("status").GetString());
        Assert.Equal(2, health.GetProperty("report").GetProperty("endpoints").GetArrayLength());
    }

    [Fact]
    public async Task Live_StreamsSnapshotsAsServerSentEvents()
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, "api/live");
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));
        using var response = await _client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, Ct);
        Assert.Equal("text/event-stream", response.Content.Headers.ContentType!.MediaType);

        using var reader = new StreamReader(await response.Content.ReadAsStreamAsync(Ct));
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        Assert.Equal("event: snapshot", await reader.ReadLineAsync(timeout.Token));
        var data = await reader.ReadLineAsync(timeout.Token);
        Assert.StartsWith("data: ", data);
        Assert.True(JsonDocument.Parse(data!["data: ".Length..]).RootElement.GetProperty("started").GetBoolean());
    }

    [Fact]
    public async Task ARequestThroughAProxy_IsRefused_UnlessRemoteAccessIsAllowed()
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, "api/overview");
        request.Headers.Add("X-Forwarded-For", "203.0.113.7");
        using var refused = await _client.SendAsync(request, Ct);
        Assert.Equal(HttpStatusCode.Forbidden, refused.StatusCode);

        var (app, client) = await StartAsync(o => o.AllowAnonymousRemoteAccess = true);
        await using var _ = app;
        using var __ = client;
        using var allowedRequest = new HttpRequestMessage(HttpMethod.Get, "api/overview");
        allowedRequest.Headers.Add("X-Forwarded-For", "203.0.113.7");
        using var allowed = await client.SendAsync(allowedRequest, Ct);
        Assert.Equal(HttpStatusCode.OK, allowed.StatusCode);
    }
}
