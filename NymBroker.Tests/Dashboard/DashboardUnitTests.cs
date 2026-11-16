using System.Net;
using System.Text;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Dashboard;
using NymBroker.Dashboard.Api;

namespace NymBroker.Tests.Dashboard;

public sealed class DashboardUnitTests
{
    // ── Access ────────────────────────────────────────────────────────────────

    private static DefaultHttpContext Request(string? remote, bool withPolicy = false, string? forwardedFor = null)
    {
        var http = new DefaultHttpContext();
        http.Connection.RemoteIpAddress = remote is null ? null : IPAddress.Parse(remote);
        if (forwardedFor is not null) http.Request.Headers["X-Forwarded-For"] = forwardedFor;
        var metadata = withPolicy ? new EndpointMetadataCollection(new AuthorizeAttribute("Ops")) : EndpointMetadataCollection.Empty;
        http.SetEndpoint(new Microsoft.AspNetCore.Http.Endpoint(_ => Task.CompletedTask, metadata, "dashboard"));
        http.RequestServices = new ServiceCollection().AddLogging().BuildServiceProvider();
        return http;
    }

    [Theory]
    [InlineData("127.0.0.1", true)]
    [InlineData("::1", true)]
    [InlineData("::ffff:127.0.0.1", true)]
    [InlineData("10.1.2.3", false)]
    [InlineData("192.168.0.5", false)]
    public void WithoutAPolicy_OnlyLocalCallersAreAllowed(string remote, bool allowed)
    {
        Assert.Equal(allowed, DashboardAccess.IsAllowed(Request(remote), new NymBrokerDashboardOptions()));
    }

    [Fact]
    public void ARequestThroughAProxy_CountsAsRemote_EvenFromLoopback()
    {
        Assert.False(DashboardAccess.IsAllowed(Request("127.0.0.1", forwardedFor: "203.0.113.7"), new NymBrokerDashboardOptions()));
    }

    [Fact]
    public void AnAuthorizationPolicy_OrTheOption_AllowsRemoteCallers()
    {
        Assert.True(DashboardAccess.IsAllowed(Request("10.1.2.3", withPolicy: true), new NymBrokerDashboardOptions()));
        Assert.True(DashboardAccess.IsAllowed(Request("10.1.2.3"), new NymBrokerDashboardOptions { AllowAnonymousRemoteAccess = true }));
    }

    // ── Options and registration ──────────────────────────────────────────────

    [Theory]
    [InlineData(nameof(NymBrokerDashboardOptions.RefreshInterval))]
    [InlineData(nameof(NymBrokerDashboardOptions.HealthInterval))]
    [InlineData(nameof(NymBrokerDashboardOptions.History))]
    [InlineData(nameof(NymBrokerDashboardOptions.MaxPayloadBytes))]
    [InlineData(nameof(NymBrokerDashboardOptions.Title))]
    public void InvalidOptions_AreRejectedWhenRegistering(string option)
    {
        Action<NymBrokerDashboardOptions> configure = option switch
        {
            nameof(NymBrokerDashboardOptions.RefreshInterval) => o => o.RefreshInterval = TimeSpan.FromMilliseconds(100),
            nameof(NymBrokerDashboardOptions.HealthInterval) => o => o.HealthInterval = TimeSpan.Zero,
            nameof(NymBrokerDashboardOptions.History) => o => o.History = TimeSpan.FromHours(2),
            nameof(NymBrokerDashboardOptions.MaxPayloadBytes) => o => o.MaxPayloadBytes = 10,
            _ => o => o.Title = " "
        };

        var error = Assert.ThrowsAny<ArgumentException>(() => new ServiceCollection().AddNymBrokerDashboard(configure));
        Assert.Equal(option, error.ParamName);
    }

    [Fact]
    public void AddNymBrokerDashboard_Twice_Throws()
    {
        var services = new ServiceCollection().AddNymBrokerDashboard();
        Assert.Throws<InvalidOperationException>(() => services.AddNymBrokerDashboard());
    }

    [Fact]
    public void MapNymBrokerDashboard_WithoutAddNymBrokerDashboard_Throws()
    {
        var app = WebApplication.CreateSlimBuilder().Build();
        Assert.Throws<InvalidOperationException>(() => app.MapNymBrokerDashboard());
    }

    // ── Message views ─────────────────────────────────────────────────────────

    private const string Envelope =
        """{"id":"m-1","correlationId":"c-1","address":{"to":"Orders","from":"Api"},"messageType":"order.created","created":"2026-11-16T10:00:00Z","traceParent":"00-abc-def-01","message":{"amount":5},"deadLetter":{"reason":"ConsumerFailed","description":"boom","deliveryCount":3}}""";

    private static QueueMessageInfo Message(byte[] payload)
        => new("row-1", QueueInspectionState.Failed, 3, DateTimeOffset.UnixEpoch, null, DateTimeOffset.UnixEpoch, "boom", payload);

    [Fact]
    public void AnEnvelope_IsReadIntoItsFields()
    {
        var view = MessageViews.From(Message(Encoding.UTF8.GetBytes(Envelope)), "Orders", showPayload: false, new NymBrokerDashboardOptions());

        var envelope = view.Envelope!;
        Assert.Equal(("m-1", "c-1", "order.created"), (envelope.Id, envelope.CorrelationId, envelope.MessageType));
        Assert.Equal(("Api", "Orders"), (envelope.From, envelope.To));
        Assert.Equal("ConsumerFailed", envelope.DeadLetter!.Reason);
        Assert.Equal(3, envelope.DeadLetter.DeliveryCount);
        Assert.Null(view.Payload);
        Assert.Equal(Envelope.Length, view.Size);
    }

    [Fact]
    public void BytesThatAreNotAnEnvelope_HaveNoEnvelope()
    {
        Assert.Null(MessageViews.ReadEnvelope("plain text"u8.ToArray()));
        Assert.Null(MessageViews.ReadEnvelope("""{"no":"type"}"""u8.ToArray()));
        Assert.Null(MessageViews.ReadEnvelope("[1,2]"u8.ToArray()));
    }

    [Fact]
    public void ThePayload_IsRedacted_AndCutToTheLimit()
    {
        var options = new NymBrokerDashboardOptions
        {
            MaxPayloadBytes = 256,
            RedactPayload = (endpoint, bytes) => Encoding.UTF8.GetBytes($"{endpoint}:{new string('x', 300)}")
        };

        var view = MessageViews.From(Message(Encoding.UTF8.GetBytes(Envelope)), "Orders", showPayload: true, options);

        Assert.True(view.PayloadTruncated);
        Assert.StartsWith("Orders:xxx", view.Payload);
        Assert.Equal(256, Encoding.UTF8.GetByteCount(view.Payload!));
        Assert.Equal("order.created", view.Envelope!.MessageType);   // read from the original bytes, not the redacted ones
    }

    [Fact]
    public void BinaryPayloads_AreShownAsBase64()
    {
        var (text, truncated) = MessageViews.AsText([0xFF, 0xFE, 0x00, 0x01], 1024);
        Assert.Equal(Convert.ToBase64String([0xFF, 0xFE, 0x00, 0x01]), text);
        Assert.False(truncated);
    }
}
