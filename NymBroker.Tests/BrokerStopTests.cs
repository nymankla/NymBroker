using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Impl;
using NymBroker.Core.Serialize;

namespace NymBroker.Tests;

/// <summary>
/// <see cref="INymBroker.StopAsync"/> stops every listener even when its token is already cancelled (the host's
/// shutdown timeout has passed) or when another listener fails to stop.
/// </summary>
public sealed class BrokerStopTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private sealed class TrackingEndPoint(Exception? stopFailure = null) : IEndPointEventDriven
    {
        private int _starts, _stops;
        public int StartCalls => Volatile.Read(ref _starts);
        public int StopCalls => Volatile.Read(ref _stops);

        public EndpointMode Mode => EndpointMode.ReadWrite;
        public Task PostAsync(byte[] message, CancellationToken ct = default) => Task.CompletedTask;
        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();

        public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct)
        {
            Interlocked.Increment(ref _starts);
            return Task.CompletedTask;
        }

        public Task StopListeningAsync()
        {
            Interlocked.Increment(ref _stops);
            return stopFailure is null ? Task.CompletedTask : Task.FromException(stopFailure);
        }
    }

    private static NymBrokerImpl CreateBroker(params IEndPoint[] endpoints) => CreateBroker(null, endpoints);

    private static NymBrokerImpl CreateBroker(CapturingLogger<NymBrokerImpl>? logger, params IEndPoint[] endpoints)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<MessageSerializerJson>();
        services.AddSingleton<IAggregator, AggregatorImpl>();
        var broker = TestBroker.Create(services.BuildServiceProvider(), logger: logger);
        for (var i = 0; i < endpoints.Length; i++)
            broker.AddEndpoint($"ep{i}", endpoints[i]);
        return broker;
    }

    [Fact]
    public async Task StopAsync_WithAnAlreadyCancelledToken_StillStopsEveryListener()
    {
        var first = new TrackingEndPoint();
        var second = new TrackingEndPoint();
        var broker = CreateBroker(first, second);
        await broker.StartAsync(Ct);

        await broker.StopAsync(new CancellationToken(canceled: true));

        Assert.Equal(1, first.StopCalls);
        Assert.Equal(1, second.StopCalls);
    }

    [Fact]
    public async Task StopAsync_WhenAListenerFailsToStop_StopsTheOthers_LogsAndRethrows()
    {
        var logger = new CapturingLogger<NymBrokerImpl>();
        var failure = new InvalidOperationException("connection lost");
        var failing = new TrackingEndPoint(failure);
        var after = new TrackingEndPoint();
        var broker = CreateBroker(logger, failing, after);
        await broker.StartAsync(Ct);

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => broker.StopAsync(Ct));

        Assert.Same(failure, thrown);
        Assert.Equal(1, after.StopCalls);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error && e.Exception == failure && e.Message.Contains("ep0", StringComparison.Ordinal));

        // The broker counts as stopped: a new start starts every listener again.
        await broker.StartAsync(Ct);
        Assert.Equal(2, after.StartCalls);
    }

    [Fact]
    public async Task StopAsync_WhenSeveralListenersFailToStop_ThrowsAnAggregateOfAll()
    {
        var first = new InvalidOperationException("first");
        var second = new TimeoutException("second");
        var broker = CreateBroker(new TrackingEndPoint(first), new TrackingEndPoint(), new TrackingEndPoint(second));
        await broker.StartAsync(Ct);

        var thrown = await Assert.ThrowsAsync<AggregateException>(() => broker.StopAsync(Ct));

        Assert.Equal(2, thrown.InnerExceptions.Count);
        Assert.Contains(first, thrown.InnerExceptions);
        Assert.Contains(second, thrown.InnerExceptions);
    }

    [Fact]
    public async Task StopAsync_WithAnAlreadyCancelledToken_LeavesTheBrokerStopped_SoItCanStartAgain()
    {
        var endpoint = new TrackingEndPoint();
        var broker = CreateBroker(endpoint);
        await broker.StartAsync(Ct);

        await broker.StopAsync(new CancellationToken(canceled: true));
        await broker.StartAsync(Ct);

        Assert.Equal(2, endpoint.StartCalls);
        await broker.StopAsync(Ct);
        Assert.Equal(2, endpoint.StopCalls);
    }
}
