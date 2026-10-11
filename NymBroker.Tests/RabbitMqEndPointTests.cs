using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Endpoint.RabbitMq;
using RabbitMQ.Client;

namespace NymBroker.Tests;

/// <summary>
/// Stopping the RabbitMQ listener (#73). The integration tests run only when NYMBROKER_RABBITMQ_HOST is set
/// (e.g. "localhost" with scripts/setup-rabbitmq.ps1); the unreachable-host test always runs.
/// </summary>
public sealed class RabbitMqEndPointTests : IAsyncLifetime
{
    private const string HostVariable = "NYMBROKER_RABBITMQ_HOST";
    private static readonly string? Host = Environment.GetEnvironmentVariable(HostVariable);
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);

    // Unique per test instance so tests can run in parallel against one broker.
    private readonly string _queue = $"nymbroker.test.{Guid.NewGuid():N}";
    private readonly List<RabbitMqEndPoint> _endpoints = [];

    // --- Always runs ---

    [Fact]
    public async Task StopListening_WhileReconnecting_EndsTheReconnectLoop()
    {
        var logger = new CapturingLogger<RabbitMqEndPoint>();
        var ep = CreateEndPoint(logger, port: 1);   // nothing listens on port 1: every connect attempt fails

        await ep.StartListeningAsync((_, _) => Task.FromResult(ProcessResult.Completed), TestContext.Current.CancellationToken);
        // One attempt takes the reconnect delay plus the failed connect (about 2 s for a refused localhost port on
        // Windows), so measure it instead of assuming it.
        await WaitUntilAsync(() => ReconnectWarnings(logger) >= 1);
        var attemptTimer = System.Diagnostics.Stopwatch.StartNew();
        await WaitUntilAsync(() => ReconnectWarnings(logger) >= 2);
        var oneAttempt = attemptTimer.Elapsed;

        await ep.StopListeningAsync().WaitAsync(Timeout, TestContext.Current.CancellationToken);
        var warningsAtStop = ReconnectWarnings(logger);
        await Task.Delay(oneAttempt * 2 + TimeSpan.FromMilliseconds(500), TestContext.Current.CancellationToken);

        Assert.Equal(warningsAtStop, ReconnectWarnings(logger));
    }

    [Fact]
    public void ConnectionFactory_Tls_IsOffByDefault()
    {
        var ssl = RabbitMqEndPoint.CreateConnectionFactory(new RabbitMqSettings()).Ssl;

        Assert.False(ssl.Enabled);
    }

    [Fact]
    public void ConnectionFactory_Tls_VerifiesTheServerCertificate()
    {
        var factory = RabbitMqEndPoint.CreateConnectionFactory(new RabbitMqSettings
        {
            HostName = "rabbit.internal",
            Port = 5671,
            UseTls = true,
            ClientCertificatePath = "client.pfx",
            ClientCertificatePassword = "secret"
        });

        Assert.True(factory.Ssl.Enabled);
        Assert.Equal("rabbit.internal", factory.Ssl.ServerName);   // defaults to HostName
        Assert.Equal(System.Net.Security.SslPolicyErrors.None, factory.Ssl.AcceptablePolicyErrors);   // nothing tolerated
        Assert.Null(factory.Ssl.CertificateValidationCallback);
        Assert.Equal("client.pfx", factory.Ssl.CertPath);
        Assert.Equal("secret", factory.Ssl.CertPassphrase);
        Assert.Equal(5671, factory.Port);
    }

    [Fact]
    public void ConnectionFactory_Tls_ServerNameOverride()
    {
        var ssl = RabbitMqEndPoint.CreateConnectionFactory(new RabbitMqSettings
        {
            HostName = "10.0.0.5", UseTls = true, TlsServerName = "rabbit.example.com"
        }).Ssl;

        Assert.Equal("rabbit.example.com", ssl.ServerName);
    }

    // --- Integration ---

    [Fact]
    public async Task StopListening_WhileHandlerRuns_WaitsAndAcksTheMessage()
    {
        RequireRabbitMq();
        var ep = CreateEndPoint();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;

        await ep.StartListeningAsync(async (_, _) =>
        {
            Interlocked.Increment(ref calls);
            started.TrySetResult();
            await release.Task;   // ignores the token on purpose: the handler finishes after stop has begun
            return ProcessResult.Completed;
        }, TestContext.Current.CancellationToken);
        await ep.PostAsync("in-flight"u8.ToArray(), TestContext.Current.CancellationToken);
        await started.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        var stop = ep.StopListeningAsync();
        await Task.Delay(200, TestContext.Current.CancellationToken);
        Assert.False(stop.IsCompleted, "StopListeningAsync must wait for the running handler.");

        release.SetResult();
        await stop.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.Equal(1, Volatile.Read(ref calls));
        Assert.Equal(0u, await ReadyMessageCountAsync());   // acked, not requeued by the channel close
    }

    [Fact]
    public async Task StopListening_AcksThePendingBatch()
    {
        RequireRabbitMq();
        var ep = CreateEndPoint(batchAckSize: 10);
        var handled = 0;

        await ep.StartListeningAsync((_, _) =>
        {
            Interlocked.Increment(ref handled);
            return Task.FromResult(ProcessResult.Completed);
        }, TestContext.Current.CancellationToken);
        for (var i = 0; i < 3; i++)
            await ep.PostAsync([(byte)i], TestContext.Current.CancellationToken);
        await WaitUntilAsync(() => Volatile.Read(ref handled) == 3);

        await ep.StopListeningAsync().WaitAsync(Timeout, TestContext.Current.CancellationToken);

        Assert.Equal(0u, await ReadyMessageCountAsync());   // 3 < BatchAckSize: before #73 all three were redelivered
    }

    [Fact]
    public async Task UseTls_AgainstThePlaintextPort_DoesNotConnect()
    {
        // Proves UseTls really reaches the connection: the container's 5672 speaks plain AMQP, so the TLS handshake fails.
        RequireRabbitMq();
        var logger = new CapturingLogger<RabbitMqEndPoint>();
        var ep = new RabbitMqEndPoint("RabbitTls",
            new RabbitMqSettings { HostName = Host!, Port = 5672, UseTls = true, ReadQueueName = _queue, ReconnectDelaySeconds = 1 }, logger);
        _endpoints.Add(ep);

        await ep.StartListeningAsync((_, _) => Task.FromResult(ProcessResult.Completed), TestContext.Current.CancellationToken);
        await WaitUntilAsync(() => ReconnectWarnings(logger) > 0);

        Assert.False(ep.HealthCheck().IsHealthy);
        Assert.DoesNotContain(logger.Entries, e => e.Message.Contains("connected to"));
    }

    [Fact]
    public async Task StopThenStart_ReceivesAgain()
    {
        RequireRabbitMq();
        var ep = CreateEndPoint();
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Func<byte[], CancellationToken, Task<ProcessResult>> handler = (_, _) =>
        {
            received.TrySetResult();
            return Task.FromResult(ProcessResult.Completed);
        };

        await ep.StartListeningAsync(handler, TestContext.Current.CancellationToken);
        await ep.StopListeningAsync().WaitAsync(Timeout, TestContext.Current.CancellationToken);
        await ep.StartListeningAsync(handler, TestContext.Current.CancellationToken);
        await ep.PostAsync("after-restart"u8.ToArray(), TestContext.Current.CancellationToken);

        await received.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);
    }

    // --- Helpers ---

    private static void RequireRabbitMq()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(Host),
            $"Set {HostVariable} to run RabbitMQ integration tests (scripts/setup-rabbitmq.ps1).");

    private RabbitMqEndPoint CreateEndPoint(CapturingLogger<RabbitMqEndPoint>? logger = null, int? port = null, int batchAckSize = 1)
    {
        var settings = new RabbitMqSettings
        {
            HostName = port is null ? Host! : "127.0.0.1",
            Port = port ?? 5672,
            ReadQueueName = _queue,
            WriteQueueName = _queue,
            BatchAckSize = batchAckSize,
            ReconnectDelaySeconds = 1
        };
        var ep = new RabbitMqEndPoint("RabbitTest", settings, logger ?? new CapturingLogger<RabbitMqEndPoint>());
        _endpoints.Add(ep);
        return ep;
    }

    private static int ReconnectWarnings(CapturingLogger<RabbitMqEndPoint> logger)
        => logger.Entries.Count(e => e.Level == LogLevel.Warning && e.Message.Contains("reconnecting"));

    private async Task<uint> ReadyMessageCountAsync()
    {
        await using var connection = await new ConnectionFactory { HostName = Host! }.CreateConnectionAsync(TestContext.Current.CancellationToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
        return (await channel.QueueDeclarePassiveAsync(_queue, TestContext.Current.CancellationToken)).MessageCount;
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("Condition not met in time.");
            await Task.Delay(25, TestContext.Current.CancellationToken);
        }
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var ep in _endpoints)
            await ep.DisposeAsync();

        if (string.IsNullOrWhiteSpace(Host)) return;
        await using var connection = await new ConnectionFactory { HostName = Host }.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();
        await channel.QueueDeleteAsync(_queue);
    }
}
