using System.Diagnostics;
using NymBroker.Core.Aggregator;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Factory;
using NymBroker.Core.Impl;
using NymBroker.Core.PubSub;
using NymBroker.Core.Serialize;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using CoreHealthCheckResult = NymBroker.Core.Endpoint.HealthCheck.HealthCheckResult;

namespace NymBroker.Tests;

/// <summary>Tests for <see cref="INymBroker.CheckHealthAsync"/> and the <see cref="NymBrokerHealthCheck"/> adapter (#53).</summary>
public sealed class BrokerHealthCheckTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private sealed class FakeEndPoint(Func<IHealthCheckResult> check, EndpointMode mode = EndpointMode.ReadWrite) : IEndPoint
    {
        public EndpointMode Mode => mode;
        public Task PostAsync(byte[] message, CancellationToken ct = default) => Task.CompletedTask;
        public IHealthCheckResult HealthCheck() => check();

        public static FakeEndPoint Healthy(EndpointMode mode = EndpointMode.ReadWrite) => new(CoreHealthCheckResult.Healthy, mode);
        public static FakeEndPoint Unhealthy(string message) => new(() => CoreHealthCheckResult.Unhealthy(message));
    }

    private static NymBrokerImpl CreateBroker(ILogger<NymBrokerImpl>? logger = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var sp = services.BuildServiceProvider();
        return new NymBrokerImpl(
            new MessageSerializerJson(),
            new AggregatorImpl(NullLogger<AggregatorImpl>.Instance),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            logger ?? NullLogger<NymBrokerImpl>.Instance);
    }

    private static async Task<NymBrokerImpl> StartedBrokerAsync(params (string Name, IEndPoint EndPoint)[] endpoints)
    {
        var broker = CreateBroker();
        foreach (var (name, ep) in endpoints)
            broker.AddEndpoint(name, ep);
        await broker.StartAsync(Ct);
        return broker;
    }

    // --- Aggregation ---

    [Fact]
    public async Task AllEndpointsHealthy_AndStarted_IsHealthy()
    {
        var broker = await StartedBrokerAsync(("B", FakeEndPoint.Healthy(EndpointMode.WriteOnly)), ("A", FakeEndPoint.Healthy()));

        var report = await broker.CheckHealthAsync(Ct);

        Assert.Equal(BrokerHealthStatus.Healthy, report.Status);
        Assert.True(report.IsHealthy);
        Assert.True(report.BrokerStarted);
        Assert.Null(report.Message);
        Assert.Equal(["A", "B"], report.Endpoints.Select(e => e.Name));
        Assert.All(report.Endpoints, e => Assert.Equal(BrokerHealthStatus.Healthy, e.Status));
        Assert.Equal(EndpointMode.WriteOnly, report.Endpoints.Single(e => e.Name == "B").Mode);
    }

    [Fact]
    public async Task CriticalEndpointUnhealthy_IsUnhealthy_AndNamesTheEndpoint()
    {
        var broker = await StartedBrokerAsync(("Ok", FakeEndPoint.Healthy()), ("Db", FakeEndPoint.Unhealthy("connection refused")));

        var report = await broker.CheckHealthAsync(Ct);

        Assert.Equal(BrokerHealthStatus.Unhealthy, report.Status);
        Assert.Contains("Db", report.Message);
        var db = report.Endpoints.Single(e => e.Name == "Db");
        Assert.Equal(BrokerHealthStatus.Unhealthy, db.Status);
        Assert.Equal("connection refused", db.Message);
        Assert.True(db.IsCritical);
    }

    [Fact]
    public async Task OnlyNonCriticalEndpointUnhealthy_IsDegraded()
    {
        var broker = await StartedBrokerAsync(("Ok", FakeEndPoint.Healthy()), ("Audit", FakeEndPoint.Unhealthy("disk full")));
        broker.ConfigureHealthCheck(new BrokerHealthCheckOptions().NonCritical("audit"));   // case-insensitive

        var report = await broker.CheckHealthAsync(Ct);

        Assert.Equal(BrokerHealthStatus.Degraded, report.Status);
        Assert.False(report.IsHealthy);
        Assert.Contains("Audit", report.Message);
        Assert.False(report.Endpoints.Single(e => e.Name == "Audit").IsCritical);
        Assert.Equal(BrokerHealthStatus.Unhealthy, report.Endpoints.Single(e => e.Name == "Audit").Status);
    }

    [Fact]
    public async Task NonCriticalAndCriticalUnhealthy_IsUnhealthy()
    {
        var broker = await StartedBrokerAsync(("Db", FakeEndPoint.Unhealthy("down")), ("Audit", FakeEndPoint.Unhealthy("disk full")));
        broker.ConfigureHealthCheck(new BrokerHealthCheckOptions().NonCritical("Audit"));

        var report = await broker.CheckHealthAsync(Ct);

        Assert.Equal(BrokerHealthStatus.Unhealthy, report.Status);
    }

    [Fact]
    public async Task BrokerNotStarted_IsUnhealthy_EvenWithHealthyEndpoints()
    {
        var broker = CreateBroker();
        broker.AddEndpoint("A", FakeEndPoint.Healthy());

        var report = await broker.CheckHealthAsync(Ct);

        Assert.Equal(BrokerHealthStatus.Unhealthy, report.Status);
        Assert.False(report.BrokerStarted);
        Assert.Contains("Broker is not running", report.Message);
        Assert.Equal(BrokerHealthStatus.Healthy, Assert.Single(report.Endpoints).Status);   // endpoints are still checked
    }

    [Fact]
    public async Task BrokerStopped_IsUnhealthy()
    {
        var broker = await StartedBrokerAsync(("A", FakeEndPoint.Healthy()));
        await broker.StopAsync(Ct);

        var report = await broker.CheckHealthAsync(Ct);

        Assert.Equal(BrokerHealthStatus.Unhealthy, report.Status);
        Assert.False(report.BrokerStarted);
    }

    [Fact]
    public async Task NoEndpoints_Started_IsHealthy()
    {
        var broker = await StartedBrokerAsync();

        var report = await broker.CheckHealthAsync(Ct);

        Assert.Equal(BrokerHealthStatus.Healthy, report.Status);
        Assert.Empty(report.Endpoints);
    }

    // --- Parallel and timeout ---

    [Fact]
    public async Task Endpoints_AreCheckedInParallel()
    {
        // Each blocks 2 s: sequential would take >= 4 s, parallel about 2 s. The margin allows a busy thread pool.
        var block = TimeSpan.FromSeconds(2);
        IHealthCheckResult Slow() { Thread.Sleep(block); return CoreHealthCheckResult.Healthy(); }
        var broker = await StartedBrokerAsync(("Slow1", new FakeEndPoint(Slow)), ("Slow2", new FakeEndPoint(Slow)));
        broker.ConfigureHealthCheck(new BrokerHealthCheckOptions { Timeout = TimeSpan.FromSeconds(30) });

        var sw = Stopwatch.StartNew();
        var report = await broker.CheckHealthAsync(Ct);
        sw.Stop();

        Assert.Equal(BrokerHealthStatus.Healthy, report.Status);
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(3.5), $"Took {sw.Elapsed}; endpoints were not checked in parallel.");
        Assert.All(report.Endpoints, e => Assert.True(e.Duration >= block - TimeSpan.FromMilliseconds(50), $"{e.Name}: {e.Duration}"));
        Assert.True(report.Duration >= block - TimeSpan.FromMilliseconds(50));
    }

    [Fact]
    public async Task EndpointSlowerThanTimeout_IsReportedTimedOut_AndCallReturnsInTime()
    {
        using var release = new ManualResetEventSlim();
        try
        {
            var logger = new CapturingLogger<NymBrokerImpl>();
            var broker = CreateBroker(logger);
            broker.AddEndpoint("Hung", new FakeEndPoint(() => { release.Wait(TimeSpan.FromSeconds(60)); return CoreHealthCheckResult.Healthy(); }));
            broker.AddEndpoint("Ok", FakeEndPoint.Healthy());
            broker.ConfigureHealthCheck(new BrokerHealthCheckOptions { Timeout = TimeSpan.FromSeconds(2) });
            await broker.StartAsync(Ct);

            var sw = Stopwatch.StartNew();
            var report = await broker.CheckHealthAsync(Ct);
            sw.Stop();

            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(6), $"Took {sw.Elapsed}; the timeout was 2 s.");
            Assert.Equal(BrokerHealthStatus.Unhealthy, report.Status);
            var hung = report.Endpoints.Single(e => e.Name == "Hung");
            Assert.Equal(BrokerHealthStatus.Unhealthy, hung.Status);
            Assert.Contains("timed out", hung.Message);
            Assert.Equal(BrokerHealthStatus.Healthy, report.Endpoints.Single(e => e.Name == "Ok").Status);
            Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("Hung"));
        }
        finally
        {
            release.Set();
        }
    }

    // --- Robustness ---

    [Fact]
    public async Task ThrowingEndpoint_IsUnhealthy_AndLogged_AndCallDoesNotThrow()
    {
        var logger = new CapturingLogger<NymBrokerImpl>();
        var broker = CreateBroker(logger);
        broker.AddEndpoint("Broken", new FakeEndPoint(() => throw new InvalidOperationException("kaboom")));
        broker.AddEndpoint("NullResult", new FakeEndPoint(() => null!));
        broker.AddEndpoint("Ok", FakeEndPoint.Healthy());
        await broker.StartAsync(Ct);

        var report = await broker.CheckHealthAsync(Ct);

        Assert.Equal(BrokerHealthStatus.Unhealthy, report.Status);
        var broken = report.Endpoints.Single(e => e.Name == "Broken");
        Assert.Equal(BrokerHealthStatus.Unhealthy, broken.Status);
        Assert.Equal("kaboom", broken.Message);
        Assert.Equal(BrokerHealthStatus.Unhealthy, report.Endpoints.Single(e => e.Name == "NullResult").Status);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning && e.Exception is InvalidOperationException);
    }

    [Fact]
    public async Task CallerCancellation_Propagates()
    {
        using var release = new ManualResetEventSlim();
        try
        {
            var broker = await StartedBrokerAsync(("Hung", new FakeEndPoint(() => { release.Wait(TimeSpan.FromSeconds(60)); return CoreHealthCheckResult.Healthy(); })));
            broker.ConfigureHealthCheck(new BrokerHealthCheckOptions { Timeout = TimeSpan.FromSeconds(30) });

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            cts.CancelAfter(TimeSpan.FromMilliseconds(200));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => broker.CheckHealthAsync(cts.Token));

            using var cancelled = new CancellationTokenSource();
            await cancelled.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => broker.CheckHealthAsync(cancelled.Token));
        }
        finally
        {
            release.Set();
        }
    }

    // --- Options and builder ---

    [Fact]
    public void Options_RejectNonPositiveTimeout()
    {
        var options = new BrokerHealthCheckOptions();
        Assert.Equal(TimeSpan.FromSeconds(10), options.Timeout);
        Assert.Throws<ArgumentOutOfRangeException>(() => options.Timeout = TimeSpan.Zero);
        Assert.Throws<ArgumentOutOfRangeException>(() => options.Timeout = TimeSpan.FromSeconds(-1));
    }

    [Fact]
    public void ConfigureHealthCheck_UnknownNonCriticalEndpoint_ThrowsAtBuild()
    {
        var builder = new ServiceCollection().AddNymBroker()
            .AddMemoryEndPoint("Mem")
            .ConfigureHealthCheck(o => o.NonCritical("Typo"));

        var ex = Assert.Throws<InvalidOperationException>(builder.Build);
        Assert.Contains("Typo", ex.Message);
    }

    [Fact]
    public async Task Builder_ConfigureHealthCheck_IsApplied()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var builder = services.AddNymBroker().AddMemoryEndPoint("Mem");
        // The way extension packages register an endpoint.
        services.AddKeyedSingleton<IEndPoint>("Audit", FakeEndPoint.Unhealthy("disk full"));
        builder.RegisterEndpoint("Audit");
        builder
            .ConfigureHealthCheck(o => o.Timeout = TimeSpan.FromSeconds(5))
            .ConfigureHealthCheck(o => o.NonCritical("Audit"))
            .Build();

        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<INymBroker>();
        await broker.StartAsync(Ct);
        try
        {
            var report = await broker.CheckHealthAsync(Ct);

            Assert.Equal(BrokerHealthStatus.Degraded, report.Status);
            Assert.Equal(["Audit", "Mem"], report.Endpoints.Select(e => e.Name));
        }
        finally
        {
            await broker.StopAsync(Ct);
        }
    }

    // --- IHealthCheck adapter ---

    [Theory]
    [InlineData(false, false, HealthStatus.Healthy)]
    [InlineData(true, false, HealthStatus.Degraded)]
    [InlineData(true, true, HealthStatus.Unhealthy)]
    public async Task Adapter_MapsStatus_AndListsEveryEndpointInData(bool auditFails, bool dbFails, HealthStatus expected)
    {
        var broker = await StartedBrokerAsync(
            ("Db", dbFails ? FakeEndPoint.Unhealthy("down") : FakeEndPoint.Healthy()),
            ("Audit", auditFails ? FakeEndPoint.Unhealthy("disk full") : FakeEndPoint.Healthy()));
        broker.ConfigureHealthCheck(new BrokerHealthCheckOptions().NonCritical("Audit"));

        var result = await new NymBrokerHealthCheck(broker).CheckHealthAsync(new HealthCheckContext(), Ct);

        Assert.Equal(expected, result.Status);
        Assert.Equal(true, result.Data[NymBrokerHealthCheck.BrokerStartedKey]);
        Assert.Equal(dbFails ? "Unhealthy: down" : "Healthy", result.Data["Db"]);
        Assert.Equal(auditFails ? "Unhealthy: disk full" : "Healthy", result.Data["Audit"]);
        Assert.Equal(3, result.Data.Count);
        if (expected != HealthStatus.Healthy)
            Assert.False(string.IsNullOrEmpty(result.Description));
    }

    [Fact]
    public async Task Adapter_BrokerNotStarted_IsUnhealthy()
    {
        var broker = CreateBroker();
        broker.AddEndpoint("A", FakeEndPoint.Healthy());

        var result = await new NymBrokerHealthCheck(broker).CheckHealthAsync(new HealthCheckContext(), Ct);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Equal(false, result.Data[NymBrokerHealthCheck.BrokerStartedKey]);
        Assert.Contains("Broker is not running", result.Description);
    }

    [Fact]
    public async Task AddHealthChecks_AddNymBroker_RunsThroughHealthCheckService()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker().AddMemoryEndPoint("Mem").Build();
        services.AddHealthChecks().AddNymBroker(name: "broker", failureStatus: HealthStatus.Degraded, tags: ["ready"]);
        await using var sp = services.BuildServiceProvider();
        var healthChecks = sp.GetRequiredService<HealthCheckService>();

        // Not started yet: unhealthy, reported with the configured failure status.
        var before = await healthChecks.CheckHealthAsync(r => r.Tags.Contains("ready"), Ct);
        Assert.Equal(HealthStatus.Degraded, before.Entries["broker"].Status);

        var broker = sp.GetRequiredService<INymBroker>();
        await broker.StartAsync(Ct);
        try
        {
            var after = await healthChecks.CheckHealthAsync(Ct);
            var entry = after.Entries["broker"];
            Assert.Equal(HealthStatus.Healthy, entry.Status);
            Assert.Equal("Healthy", entry.Data["Mem"]);
            Assert.Equal(true, entry.Data[NymBrokerHealthCheck.BrokerStartedKey]);
        }
        finally
        {
            await broker.StopAsync(Ct);
        }
    }
}

/// <summary>Builds a broker through DI, starts it, runs <see cref="INymBroker.CheckHealthAsync"/> and stops it.
/// Used by the transport test classes (Postgres, SQL Server, Service Bus).</summary>
internal static class BrokerHealthTestHelper
{
    public static async Task<BrokerHealthReport> CheckAsync(Action<NymBrokerBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var builder = services.AddNymBroker();
        configure(builder);
        builder.Build();

        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<INymBroker>();
        await broker.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            return await broker.CheckHealthAsync(TestContext.Current.CancellationToken);
        }
        finally
        {
            await broker.StopAsync(TestContext.Current.CancellationToken);
        }
    }
}
