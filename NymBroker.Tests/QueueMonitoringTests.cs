using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Time.Testing;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Core.Impl;
using NymBroker.Endpoint.Sqlite;

namespace NymBroker.Tests;

/// <summary>Queue depth and age gauges and dead-letter thresholds (#133).</summary>
public sealed class QueueMonitoringTests
{
    private static readonly DateTimeOffset Now = new(2026, 11, 20, 12, 0, 0, TimeSpan.Zero);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>A queue endpoint whose inspector answers what the test sets.</summary>
    private sealed class FakeQueue : IEndPoint, IQueueInspector
    {
        public QueueCounts Counts { get; set; } = new(0, 0, 0, 0, 0);
        public DateTimeOffset OldestPendingCreated { get; set; } = Now;
        public DateTimeOffset OldestFailedAt { get; set; } = Now;
        public Exception? Failure { get; set; }

        public Task PostAsync(byte[] message, CancellationToken ct = default) => Task.CompletedTask;
        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();

        public Task<QueueCounts> GetCountsAsync(CancellationToken ct = default)
            => Failure is null ? Task.FromResult(Counts) : Task.FromException<QueueCounts>(Failure);

        public Task<QueuePage> GetMessagesAsync(QueueInspectionState state, int limit = 100, string? cursor = null,
            DateTimeOffset? failedSince = null, CancellationToken ct = default)
        {
            var message = state == QueueInspectionState.Failed
                ? new QueueMessageInfo("f1", state, 3, OldestFailedAt.AddMinutes(-5), null, OldestFailedAt, "ConsumerFailed: boom", default)
                : new QueueMessageInfo("p1", state, 0, OldestPendingCreated, null, null, null, default);
            return Task.FromResult(new QueuePage([message], null));
        }
    }

    /// <summary>A plain endpoint without an inspector.</summary>
    private sealed class PlainEndpoint : IEndPoint
    {
        public Task PostAsync(byte[] message, CancellationToken ct = default) => Task.CompletedTask;
        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
    }

    private static (NymBrokerImpl Broker, FakeTimeProvider Time) CreateBroker(Action<QueueMonitoringOptions>? configure,
        params (string Name, IEndPoint Endpoint)[] endpoints)
    {
        var broker = TestBroker.Create(new ServiceCollection().AddLogging().BuildServiceProvider());
        foreach (var (name, endpoint) in endpoints) broker.AddEndpoint(name, endpoint);
        var options = new QueueMonitoringOptions();
        configure?.Invoke(options);
        var time = new FakeTimeProvider(Now);
        broker.EnableQueueMonitoring(options, time);
        return (broker, time);
    }

    private static string Unique(string name) => $"{name}{Guid.NewGuid():N}";

    // --- Sampling ---

    [Fact]
    public async Task Sample_ReadsCountsAndTheOldestPendingAndFailedMessage()
    {
        var queue = new FakeQueue
        {
            Counts = new(Pending: 4, InProgress: 1, Deferred: 2, Failed: 3, Completed: 10),
            OldestPendingCreated = Now.AddMinutes(-10),
            OldestFailedAt = Now.AddHours(-2)
        };
        var (broker, _) = CreateBroker(null, ("Orders", queue), ("Plain", new PlainEndpoint()));

        await broker.QueueMonitor!.SampleAsync(Ct);

        var sample = Assert.Single(broker.QueueMonitor.Samples).Value;
        Assert.Equal(queue.Counts, sample.Counts);
        Assert.Equal(Now.AddMinutes(-10), sample.OldestPendingAt);
        Assert.Equal(Now.AddHours(-2), sample.OldestFailedAt);   // when it failed, not when it was posted
    }

    [Fact]
    public async Task Sample_EmptyStates_HaveNoOldestMessage()
    {
        var (broker, _) = CreateBroker(null, ("Orders", new FakeQueue()));

        await broker.QueueMonitor!.SampleAsync(Ct);

        var sample = broker.QueueMonitor.Samples["Orders"];
        Assert.Null(sample.OldestPendingAt);
        Assert.Null(sample.OldestFailedAt);
    }

    [Fact]
    public async Task Sample_AFailingInspector_IsLeftOut_AndTheOthersAreSampled()
    {
        var failing = new FakeQueue { Counts = new(1, 0, 0, 0, 0) };
        var (broker, _) = CreateBroker(null, ("Broken", failing), ("Orders", new FakeQueue()));
        await broker.QueueMonitor!.SampleAsync(Ct);
        Assert.Contains("Broken", broker.QueueMonitor.Samples.Keys);

        failing.Failure = new InvalidOperationException("database down");
        await broker.QueueMonitor.SampleAsync(Ct);

        Assert.Equal(["Orders"], broker.QueueMonitor.Samples.Keys);   // no stale numbers for Broken
    }

    [Fact]
    public async Task Run_SamplesAtOnce_ThenEveryInterval()
    {
        var queue = new FakeQueue { Counts = new(1, 0, 0, 0, 0) };
        var (broker, time) = CreateBroker(o => o.Interval = TimeSpan.FromSeconds(30), ("Orders", queue));
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);

        var run = broker.QueueMonitor!.RunAsync(cts.Token);
        await TestWait.UntilAsync(() => Task.FromResult(broker.QueueMonitor.Samples.ContainsKey("Orders")), TimeSpan.FromSeconds(5));
        queue.Counts = new(7, 0, 0, 0, 0);
        time.Advance(TimeSpan.FromSeconds(30));
        await TestWait.UntilAsync(() => Task.FromResult(broker.QueueMonitor.Samples["Orders"].Counts.Pending == 7), TimeSpan.FromSeconds(5));

        await cts.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(5), Ct);   // ends cleanly on its own token
    }

    // --- Gauges ---

    [Fact]
    public async Task Gauges_ReportTheLastSample()
    {
        var name = Unique("Orders");
        var queue = new FakeQueue
        {
            Counts = new(Pending: 4, InProgress: 1, Deferred: 2, Failed: 3, Completed: 10),
            OldestPendingCreated = Now.AddMinutes(-10),
            OldestFailedAt = Now.AddHours(-2)
        };
        var (broker, _) = CreateBroker(null, (name, queue));
        await broker.QueueMonitor!.SampleAsync(Ct);

        var counts = new Dictionary<string, long>();
        var ages = new Dictionary<string, double>();
        using (broker.QueueMonitor.Publish())
        using (var listener = new MeterListener())
        {
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == "NymBroker" && instrument.Name.StartsWith("nymbroker.queue.", StringComparison.Ordinal))
                    l.EnableMeasurementEvents(instrument);
            };
            listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                if (Tag(tags, "endpoint") == name) counts[Tag(tags, "state")!] = value;
            });
            listener.SetMeasurementEventCallback<double>((_, value, tags, _) =>
            {
                if (Tag(tags, "endpoint") == name) ages[Tag(tags, "state")!] = value;
            });
            listener.Start();
            listener.RecordObservableInstruments();
        }

        Assert.Equal(new Dictionary<string, long> { ["pending"] = 4, ["in_progress"] = 1, ["deferred"] = 2, ["failed"] = 3 }, counts);
        Assert.Equal(600, ages["pending"]);
        Assert.Equal(7200, ages["failed"]);
    }

    [Fact]
    public async Task Gauges_ReportNothing_AfterTheMonitorStops()
    {
        var name = Unique("Orders");
        var (broker, _) = CreateBroker(null, (name, new FakeQueue { Counts = new(1, 0, 0, 0, 0) }));
        await broker.QueueMonitor!.SampleAsync(Ct);
        broker.QueueMonitor.Publish().Dispose();

        var seen = false;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Name == "nymbroker.queue.messages") l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, _, tags, _) => seen |= Tag(tags, "endpoint") == name);
        listener.Start();
        listener.RecordObservableInstruments();

        Assert.False(seen);
    }

    private static string? Tag(ReadOnlySpan<KeyValuePair<string, object?>> tags, string key)
    {
        foreach (var tag in tags)
            if (tag.Key == key) return tag.Value as string;
        return null;
    }

    // --- Dead-letter thresholds and health ---

    [Fact]
    public async Task Health_OverTheCountThreshold_IsDegraded()
    {
        var queue = new FakeQueue { Counts = new(0, 0, 0, Failed: 3, 0), OldestFailedAt = Now.AddHours(-2) };
        var (broker, _) = CreateBroker(o => o.DeadLetterThreshold("Orders", maxCount: 0), ("Orders", queue), ("Other", new FakeQueue()));
        await broker.QueueMonitor!.SampleAsync(Ct);
        await broker.StartAsync(Ct);

        var report = await broker.CheckHealthAsync(Ct);

        Assert.Equal(BrokerHealthStatus.Degraded, report.Status);
        var orders = report.Endpoints.Single(e => e.Name == "Orders");
        Assert.Equal(BrokerHealthStatus.Degraded, orders.Status);
        Assert.Equal("3 dead letter(s) (threshold 0), oldest 2.0 h", orders.Message);
        Assert.Contains("dead-letter threshold: Orders", report.Message);
        Assert.Equal(BrokerHealthStatus.Healthy, report.Endpoints.Single(e => e.Name == "Other").Status);
        await broker.StopAsync(Ct);
    }

    [Fact]
    public async Task Health_UnderTheThreshold_IsHealthy()
    {
        var queue = new FakeQueue { Counts = new(0, 0, 0, Failed: 2, 0), OldestFailedAt = Now.AddMinutes(-5) };
        var (broker, _) = CreateBroker(o => o.DeadLetterThreshold("Orders", maxCount: 2, maxAge: TimeSpan.FromHours(1)), ("Orders", queue));
        await broker.QueueMonitor!.SampleAsync(Ct);
        await broker.StartAsync(Ct);

        Assert.Equal(BrokerHealthStatus.Healthy, (await broker.CheckHealthAsync(Ct)).Status);
        await broker.StopAsync(Ct);
    }

    [Fact]
    public async Task Health_OverTheAgeThreshold_IsDegraded_AndTheAgeGrowsBetweenSamples()
    {
        var queue = new FakeQueue { Counts = new(0, 0, 0, Failed: 1, 0), OldestFailedAt = Now.AddMinutes(-50) };
        var (broker, time) = CreateBroker(o => o.DeadLetterThreshold("Orders", maxAge: TimeSpan.FromHours(1)), ("Orders", queue));
        await broker.QueueMonitor!.SampleAsync(Ct);

        Assert.Null(broker.QueueMonitor.DeadLetterBreach("Orders"));
        time.Advance(TimeSpan.FromMinutes(15));   // no new sample: the age is computed from the oldest message's time
        Assert.Equal("1 dead letter(s) (threshold older than 60 min), oldest 65 min", broker.QueueMonitor.DeadLetterBreach("Orders"));
    }

    [Fact]
    public async Task Health_OnTheDeadLetterEndpoint_ItsWaitingMessagesAreDeadLetters()
    {
        var deadLetters = new FakeQueue { Counts = new(Pending: 2, 0, 0, 0, 0), OldestPendingCreated = Now.AddHours(-3) };
        var (broker, _) = CreateBroker(o => o.DeadLetterThreshold("DeadLetters", maxCount: 0), ("DeadLetters", deadLetters));
        broker.SetDeadLetterEndpoint("DeadLetters");
        await broker.QueueMonitor!.SampleAsync(Ct);

        Assert.Equal("2 dead letter(s) (threshold 0), oldest 3.0 h", broker.QueueMonitor.DeadLetterBreach("DeadLetters"));
    }

    [Fact]
    public async Task Health_AnUnreachableEndpoint_StaysUnhealthy()
    {
        var queue = new FakeQueue { Counts = new(0, 0, 0, Failed: 5, 0) };
        var (broker, _) = CreateBroker(o => o.DeadLetterThreshold("Orders", maxCount: 0), ("Orders", new UnreachableQueue(queue)));
        await broker.QueueMonitor!.SampleAsync(Ct);
        await broker.StartAsync(Ct);

        var report = await broker.CheckHealthAsync(Ct);

        Assert.Equal(BrokerHealthStatus.Unhealthy, report.Status);
        Assert.Equal("down", report.Endpoints.Single().Message);
        await broker.StopAsync(Ct);
    }

    private sealed class UnreachableQueue(FakeQueue inner) : IEndPoint, IQueueInspector
    {
        public Task PostAsync(byte[] message, CancellationToken ct = default) => Task.CompletedTask;
        public IHealthCheckResult HealthCheck() => HealthCheckResult.Unhealthy("down");
        public Task<QueueCounts> GetCountsAsync(CancellationToken ct = default) => inner.GetCountsAsync(ct);
        public Task<QueuePage> GetMessagesAsync(QueueInspectionState state, int limit = 100, string? cursor = null,
            DateTimeOffset? failedSince = null, CancellationToken ct = default) => inner.GetMessagesAsync(state, limit, cursor, failedSince, ct);
    }

    [Fact]
    public async Task Health_WithoutASample_IsNotChanged()
    {
        var (broker, _) = CreateBroker(o => o.DeadLetterThreshold("Orders", maxCount: 0), ("Orders", new FakeQueue { Counts = new(0, 0, 0, 9, 0) }));
        await broker.StartAsync(Ct);

        Assert.Equal(BrokerHealthStatus.Healthy, (await broker.CheckHealthAsync(Ct)).Status);
        await broker.StopAsync(Ct);
    }

    // --- Options and builder ---

    [Fact]
    public void Options_Validate()
    {
        var options = new QueueMonitoringOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => options.Interval = TimeSpan.FromMilliseconds(500));
        Assert.Throws<ArgumentException>(() => options.DeadLetterThreshold("Orders"));
        Assert.Throws<ArgumentOutOfRangeException>(() => options.DeadLetterThreshold("Orders", maxCount: -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => options.DeadLetterThreshold("Orders", maxAge: TimeSpan.Zero));

        options.DeadLetterThreshold("orders", maxCount: 1).DeadLetterThreshold("ORDERS", maxAge: TimeSpan.FromHours(1));
        Assert.Equal(new DeadLetterThreshold(null, TimeSpan.FromHours(1)), Assert.Single(options.DeadLetterThresholds).Value);
    }

    [Fact]
    public void Builder_ThresholdForAnUnknownEndpoint_Throws()
    {
        var builder = new ServiceCollection().AddNymBroker()
            .AddMemoryEndPoint("Orders")
            .MonitorQueues(o => o.DeadLetterThreshold("Nope", maxCount: 0));

        Assert.Contains("Nope", Assert.Throws<InvalidOperationException>(builder.Build).Message);
    }

    [Fact]
    public async Task Builder_MonitorQueues_SamplesASqliteQueue()
    {
        var folder = Directory.CreateTempSubdirectory("nymbroker-monitor-");
        try
        {
            var services = new ServiceCollection().AddLogging();
            services.AddNymBroker()
                .AddSqliteEndPoint("Orders", new SqliteSettings { ConnectionString = $"Data Source={Path.Combine(folder.FullName, "q.db")}", TableName = "Orders" },
                    EndpointMode.WriteOnly)
                .MonitorQueues(o => o.DeadLetterThreshold("Orders", maxCount: 0))
                .Build();
            await using var sp = services.BuildServiceProvider();
            Assert.Contains(sp.GetServices<IHostedService>(), s => s.GetType().Name == "QueueMonitorService");

            var broker = sp.GetRequiredService<NymBrokerImpl>();
            await broker.PostAsync("Orders", new { Id = 1 }, Ct);
            await broker.PostAsync("Orders", new { Id = 2 }, Ct);
            await broker.QueueMonitor!.SampleAsync(Ct);

            var sample = broker.QueueMonitor.Samples["Orders"];
            Assert.Equal(2, sample.Counts.Pending);
            Assert.NotNull(sample.OldestPendingAt);
            Assert.Null(broker.QueueMonitor.DeadLetterBreach("Orders"));
        }
        finally
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            folder.Delete(recursive: true);
        }
    }

    [Fact]
    public void Builder_WithoutMonitorQueues_RegistersNoMonitor()
    {
        var services = new ServiceCollection().AddLogging();
        services.AddNymBroker().AddMemoryEndPoint("Orders").Build();
        using var sp = services.BuildServiceProvider();

        Assert.DoesNotContain(sp.GetServices<IHostedService>(), s => s.GetType().Name == "QueueMonitorService");
        Assert.Null(sp.GetRequiredService<NymBrokerImpl>().QueueMonitor);
    }
}
