using System.Collections.Concurrent;
using System.Text;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.AzureServiceBus;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;
using NymBroker.Core.Endpoint.HealthCheck;

namespace NymBroker.Tests;

/// <summary>
/// Tests for <see cref="AzureServiceBusEndPoint"/>. Unit tests always run; integration tests need the Service Bus
/// emulator (<c>scripts/setup-servicebus.ps1</c>) and are skipped unless <c>NYMBROKER_SERVICEBUS_CS</c> is set, e.g.
/// <c>Endpoint=sb://localhost:5673;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;</c>.
/// Integration tests share the emulator's <c>nymbroker.tests</c> queue (MaxDeliveryCount 3); tests in one class run
/// sequentially and each starts by draining the queue and its dead-letter queue.
/// </summary>
public sealed class AzureServiceBusEndPointTests : IAsyncLifetime
{
    private const string ConnectionStringVariable = "NYMBROKER_SERVICEBUS_CS";
    private const string TestQueue = "nymbroker.tests";
    private const string TestTopic = "nymbroker.tests.topic";
    private const string TestSubscription = "all";
    private static readonly string? ConnectionString = Environment.GetEnvironmentVariable(ConnectionStringVariable);
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private readonly List<AzureServiceBusEndPoint> _endpoints = [];

    // =====================================================================
    // Unit tests (always run)
    // =====================================================================

    public static TheoryData<string, AzureServiceBusSettings> InvalidSettings => new()
    {
        { "neither connection", new AzureServiceBusSettings { QueueName = "q" } },
        { "both connection kinds", new AzureServiceBusSettings { ConnectionString = "cs", FullyQualifiedNamespace = "ns.servicebus.windows.net", QueueName = "q" } },
        { "namespace without credential", new AzureServiceBusSettings { FullyQualifiedNamespace = "ns.servicebus.windows.net", QueueName = "q" } },
        { "no entity", new AzureServiceBusSettings { ConnectionString = "cs" } },
        { "queue and topic", new AzureServiceBusSettings { ConnectionString = "cs", QueueName = "q", TopicName = "t" } },
        { "subscription on queue", new AzureServiceBusSettings { ConnectionString = "cs", QueueName = "q", SubscriptionName = "s" } },
        { "zero concurrency", new AzureServiceBusSettings { ConnectionString = "cs", QueueName = "q", MaxConcurrentCalls = 0 } },
    };

    [Theory]
    [MemberData(nameof(InvalidSettings))]
    public void Settings_Invalid_Throw(string _, AzureServiceBusSettings settings)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => settings.Validate("Sb"));
        Assert.Contains("'Sb'", ex.Message);
    }

    [Fact]
    public void Settings_TopicWithoutSubscription_IsValidForSending()
    {
        new AzureServiceBusSettings { ConnectionString = "cs", TopicName = "t" }.Validate("Sb");
    }

    [Fact]
    public async Task StartListening_TopicWithoutSubscription_Throws()
    {
        await using var ep = new AzureServiceBusEndPoint("Sb",
            new AzureServiceBusSettings { ConnectionString = UnreachableConnectionString, TopicName = "t" },
            new CapturingLogger<AzureServiceBusEndPoint>());

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            ep.StartListeningAsync((_, _) => Task.FromResult(ProcessResult.Completed), TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task UsesNativeDeadLetter_FollowsSetting(bool useNative)
    {
        await using var ep = new AzureServiceBusEndPoint("Sb",
            new AzureServiceBusSettings { ConnectionString = UnreachableConnectionString, QueueName = "q", UseNativeDeadLetter = useNative },
            new CapturingLogger<AzureServiceBusEndPoint>());

        Assert.Equal(useNative, ((IEndPointEventDriven)ep).UsesNativeDeadLetter);
    }

    private sealed class FakeSettler : IServiceBusMessageSettler
    {
        public List<string> Calls { get; } = [];
        public Task CompleteAsync(CancellationToken ct) { Calls.Add("complete"); return Task.CompletedTask; }
        public Task AbandonAsync(CancellationToken ct) { Calls.Add("abandon"); return Task.CompletedTask; }
        public Task DeadLetterAsync(string reason, string? description, CancellationToken ct)
        {
            Calls.Add($"deadletter:{reason}:{description}");
            return Task.CompletedTask;
        }
    }

    public static TheoryData<string, bool, string> SettlementCases => new()
    {
        { "completed", false, "complete" },
        { "retry", false, "abandon" },
        { "deadletter", false, "deadletter:Expired:too old" },
        { "completed", true, "complete" },
        { "retry", true, "abandon" },
        { "deadletter", true, "complete" },   // already in the DLQ: removed, not dead-lettered again
    };

    [Theory]
    [MemberData(nameof(SettlementCases))]
    public async Task Settlement_MapsResultToServiceBusOperation(string outcome, bool readingDeadLetterQueue, string expectedCall)
    {
        var result = outcome switch
        {
            "completed" => ProcessResult.Completed,
            "retry" => ProcessResult.Retry(new InvalidOperationException("transient")),
            _ => ProcessResult.DeadLetter(DeadLetterReasons.Expired, "too old")
        };
        var settler = new FakeSettler();
        var logger = new CapturingLogger<AzureServiceBusEndPoint>();

        await ServiceBusSettlement.SettleAsync(result, readingDeadLetterQueue, settler, logger, "Sb", "msg-1", 1, TestContext.Current.CancellationToken);

        Assert.Equal([expectedCall], settler.Calls);
        if (outcome != "completed")
            Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning);
    }

    [Fact]
    public void Settlement_TruncatesDescriptionToServiceBusLimit()
    {
        Assert.Equal(ServiceBusSettlement.MaxDeadLetterPropertyLength, ServiceBusSettlement.Truncate(new string('x', 10_000))!.Length);
        Assert.Equal("short", ServiceBusSettlement.Truncate("short"));
        Assert.Null(ServiceBusSettlement.Truncate(null));
    }

    [Fact]
    public async Task HealthCheck_UnreachableNamespace_ReturnsUnhealthyWithoutThrowing()
    {
        var logger = new CapturingLogger<AzureServiceBusEndPoint>();
        await using var ep = new AzureServiceBusEndPoint("Sb",
            new AzureServiceBusSettings { ConnectionString = UnreachableConnectionString, QueueName = "q" }, logger);

        var result = ep.HealthCheck();

        Assert.False(result.IsHealthy);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error);
    }

    [Fact]
    public async Task AddAzureServiceBusEndPoint_RegistersEndpoint()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .AddAzureServiceBusEndPoint("Sb", new AzureServiceBusSettings { ConnectionString = UnreachableConnectionString, QueueName = "q" })
            .Build();

        await using var sp = services.BuildServiceProvider();

        Assert.IsType<AzureServiceBusEndPoint>(sp.GetRequiredKeyedService<IEndPoint>("Sb"));
    }

    [Fact]
    public void AddAzureServiceBusEndPoint_InvalidSettings_ThrowsAtRegistration()
    {
        var builder = new ServiceCollection().AddNymBroker();
        Assert.Throws<InvalidOperationException>(() => builder.AddAzureServiceBusEndPoint("Sb", new AzureServiceBusSettings()));
    }

    [Fact]
    public async Task WithAzureServiceBus_RegistersConfiguredEndpoint_CaseInsensitively()
    {
        var json = $$"""
            {
              "NymBroker": {
                "Endpoints": [
                  { "name": "Sb1", "type": "azureservicebus",
                    "config": { "connectionString": "{{UnreachableConnectionString}}", "queueName": "orders", "maxConcurrentCalls": 4 } }
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
            services.AddNymBroker().LoadConfiguration(path).WithAzureServiceBus().Build();

            await using var sp = services.BuildServiceProvider();

            Assert.IsType<AzureServiceBusEndPoint>(sp.GetRequiredKeyedService<IEndPoint>("Sb1"));
        }
        finally
        {
            File.Delete(path);
        }
    }

    // =====================================================================
    // Integration tests (emulator)
    // =====================================================================

    [Fact]
    public async Task PostedMessage_ReachesHandler_WithBytesUnchanged_AndIsCompleted()
    {
        RequireServiceBus();
        var ep = CreateEndPoint();
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var payload = Encoding.UTF8.GetBytes("""{"id":"1","message":{"text":"åäö"}}""");

        await ep.StartListeningAsync((raw, _) => { received.TrySetResult(raw); return Task.FromResult(ProcessResult.Completed); },
            TestContext.Current.CancellationToken);
        await ep.PostAsync(payload, TestContext.Current.CancellationToken);

        Assert.Equal(payload, await received.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken));
        await ep.StopListeningAsync();
        Assert.Null(await PeekAsync(SubQueue.None));
    }

    [Fact]
    public async Task RetryResult_IsRedelivered_UntilMaxDeliveryCount_ThenDeadLetteredByServiceBus()
    {
        RequireServiceBus();
        var ep = CreateEndPoint();
        var calls = 0;

        await ep.StartListeningAsync((_, _) =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(ProcessResult.Retry(new InvalidOperationException("transient")));
        }, TestContext.Current.CancellationToken);
        await ep.PostAsync("retry-me"u8.ToArray(), TestContext.Current.CancellationToken);

        var deadLettered = await ReceiveDeadLetterAsync();
        await ep.StopListeningAsync();

        Assert.Equal("retry-me", deadLettered.Body.ToString());
        Assert.Equal("MaxDeliveryCountExceeded", deadLettered.DeadLetterReason);
        Assert.Equal(3, Volatile.Read(ref calls));   // MaxDeliveryCount of nymbroker.tests
    }

    [Fact]
    public async Task DeadLetterResult_MovesMessageToDeadLetterQueue_WithReason()
    {
        RequireServiceBus();
        var ep = CreateEndPoint();
        var calls = 0;

        await ep.StartListeningAsync((_, _) =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(ProcessResult.DeadLetter(DeadLetterReasons.Expired, "too old"));
        }, TestContext.Current.CancellationToken);
        await ep.PostAsync("dead"u8.ToArray(), TestContext.Current.CancellationToken);

        var deadLettered = await ReceiveDeadLetterAsync();
        await ep.StopListeningAsync();

        Assert.Equal(DeadLetterReasons.Expired, deadLettered.DeadLetterReason);
        Assert.Equal("too old", deadLettered.DeadLetterErrorDescription);
        Assert.Equal(1, Volatile.Read(ref calls));
    }

    [Fact]
    public async Task ThrowingHandler_CountsAsRetry_AndTheRedeliveryIsProcessed()
    {
        RequireServiceBus();
        var logger = new CapturingLogger<AzureServiceBusEndPoint>();
        var ep = CreateEndPoint(logger: logger);
        var calls = 0;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.StartListeningAsync((_, _) =>
        {
            if (Interlocked.Increment(ref calls) == 1)
                throw new InvalidOperationException("boom");
            done.TrySetResult();
            return Task.FromResult(ProcessResult.Completed);
        }, TestContext.Current.CancellationToken);
        await ep.PostAsync("throw-once"u8.ToArray(), TestContext.Current.CancellationToken);

        await done.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);
        await ep.StopListeningAsync();

        Assert.Equal(2, Volatile.Read(ref calls));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error && e.Message.Contains("Unhandled error dispatching"));
        Assert.Null(await PeekAsync(SubQueue.DeadLetter));
    }

    [Fact]
    public async Task AfterStopListening_HandlerIsNeverCalled_AndMessageStaysQueued()
    {
        RequireServiceBus();
        var ep = CreateEndPoint();
        var calls = 0;
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.StartListeningAsync((_, _) =>
        {
            Interlocked.Increment(ref calls);
            first.TrySetResult();
            return Task.FromResult(ProcessResult.Completed);
        }, TestContext.Current.CancellationToken);
        await ep.PostAsync("before-stop"u8.ToArray(), TestContext.Current.CancellationToken);
        await first.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);

        await ep.StopListeningAsync();
        var callsAtStop = Volatile.Read(ref calls);
        await ep.PostAsync("after-stop"u8.ToArray(), TestContext.Current.CancellationToken);
        await Task.Delay(1000, TestContext.Current.CancellationToken);

        Assert.Equal(callsAtStop, Volatile.Read(ref calls));
        Assert.Equal("after-stop", (await PeekAsync(SubQueue.None))?.Body.ToString());

        // Don't leave the message for the next test (the next test's drain would also catch it).
        await using var client = new ServiceBusClient(ConnectionString);
        await DrainAsync(() => client.CreateReceiver(TestQueue));
    }

    [Fact]
    public async Task ReadDeadLetterQueue_ReceivesDeadLetteredMessages_AndRemovesThemOnDeadLetterResult()
    {
        RequireServiceBus();
        var ep = CreateEndPoint();
        await ep.StartListeningAsync((_, _) => Task.FromResult(ProcessResult.DeadLetter(DeadLetterReasons.DeserializationFailed, "bad bytes")),
            TestContext.Current.CancellationToken);
        await ep.PostAsync("to-dlq"u8.ToArray(), TestContext.Current.CancellationToken);
        await WaitUntilAsync(async () => await PeekAsync(SubQueue.DeadLetter) is not null);
        await ep.StopListeningAsync();

        var dlqReader = CreateEndPoint(readDeadLetterQueue: true);
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        await dlqReader.StartListeningAsync((raw, _) =>
        {
            received.TrySetResult(raw);
            return Task.FromResult(ProcessResult.DeadLetter(DeadLetterReasons.DeserializationFailed, "still bad"));
        }, TestContext.Current.CancellationToken);

        var body = await received.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);
        var ctx = (RawMessageContext)new MessageSerializerJson().Deserialize(body);
        Assert.Equal(DeadLetterReasons.DeserializationFailed, ctx.DeadLetter!.Reason);
        Assert.Equal("bad bytes", ctx.DeadLetter.Description);
        Assert.Equal("to-dlq", Encoding.UTF8.GetString(Convert.FromBase64String(
            MessageSerializerJson.DeserializeMessage<UndecodableMessage>(ctx)!.PayloadBase64)));
        await WaitUntilAsync(async () => await PeekAsync(SubQueue.DeadLetter) is null);
        await dlqReader.StopListeningAsync();
    }

    [Fact]
    public void BuildBody_ForDeadLetterQueue_AnnotatesWithServiceBusReason()
    {
        var original = Encoding.UTF8.GetBytes("""{"id":"2f1b8d7e-0000-0000-0000-000000000001","messageType":"x","message":{"v":1}}""");
        var enqueued = new DateTimeOffset(2026, 10, 5, 12, 0, 0, TimeSpan.Zero);
        var message = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromBytes(original), deliveryCount: 3, enqueuedTime: enqueued, deadLetterSource: "orders",
            properties: new Dictionary<string, object>
            {
                ["DeadLetterReason"] = "MaxDeliveryCountExceeded",
                ["DeadLetterErrorDescription"] = "too many"
            });

        var plain = AzureServiceBusEndPoint.BuildBody(message, false, "Sb");
        Assert.Equal(original, plain);

        var ctx = new MessageSerializerJson().Deserialize(AzureServiceBusEndPoint.BuildBody(message, true, "Sb"));
        Assert.Equal("MaxDeliveryCountExceeded", ctx.DeadLetter!.Reason);
        Assert.Equal("too many", ctx.DeadLetter.Description);
        Assert.Equal("orders", ctx.DeadLetter.SourceEndpoint);
        Assert.Equal(3, ctx.DeadLetter.DeliveryCount);
        Assert.Equal(enqueued.UtcDateTime, ctx.DeadLetter.DeadLetteredAt);
    }

    [Fact]
    public async Task Topic_SendAndReceiveThroughSubscription()
    {
        RequireServiceBus();
        var sender = CreateEndPoint(topic: true, subscription: false);
        var receiver = CreateEndPoint(topic: true, subscription: true);
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        await receiver.StartListeningAsync((raw, _) =>
        {
            received.TrySetResult(Encoding.UTF8.GetString(raw));
            return Task.FromResult(ProcessResult.Completed);
        }, TestContext.Current.CancellationToken);
        await sender.PostAsync("via-topic"u8.ToArray(), TestContext.Current.CancellationToken);

        Assert.Equal("via-topic", await received.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken));
        Assert.True(sender.HealthCheck().IsHealthy);
        Assert.True(receiver.HealthCheck().IsHealthy);
        await receiver.StopListeningAsync();
    }

    [MessageName("tests.servicebus.order")]
    public sealed class SbOrder { public int Id { get; set; } }

    public sealed class FailingSbOrderConsumer : IConsume<SbOrder>
    {
        public static int Calls;
        public Task ConsumeAsync(SbOrder message, IMessageContext context, CancellationToken ct = default)
        {
            Interlocked.Increment(ref Calls);
            throw new InvalidOperationException("consumer failed");
        }
    }

    [Fact]
    public async Task ThroughBroker_ConsumerFailure_IsRetriedThenDeadLetteredByServiceBus()
    {
        RequireServiceBus();
        Interlocked.Exchange(ref FailingSbOrderConsumer.Calls, 0);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .AddAzureServiceBusEndPoint("Sb", new AzureServiceBusSettings { ConnectionString = ConnectionString, QueueName = TestQueue })
            .AddMemoryEndPoint("DLQ", mode: EndpointMode.WriteOnly)
            .WithDeadLetterEndpoint("DLQ")
            .AddConsumer<FailingSbOrderConsumer>()
            .Build();
        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<INymBroker>();

        await broker.StartAsync(TestContext.Current.CancellationToken);
        await broker.PostAsync("Sb", new SbOrder { Id = 1 }, TestContext.Current.CancellationToken);
        var deadLettered = await ReceiveDeadLetterAsync();
        await broker.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal("MaxDeliveryCountExceeded", deadLettered.DeadLetterReason);
        Assert.Equal(3, Volatile.Read(ref FailingSbOrderConsumer.Calls));
        var brokerDlq = (MemoryQueueEndPoint)sp.GetRequiredKeyedService<IEndPoint>("DLQ");
        await foreach (var _ in brokerDlq.ReadAsync(TestContext.Current.CancellationToken))
            Assert.Fail("The broker's dead-letter endpoint should stay empty for a native dead-letter source.");
    }

    [Fact]
    public async Task PostBatch_LargerThanOneServiceBusBatch_IsChunked_AndArrivesInOrder()
    {
        RequireServiceBus();
        var ep = CreateEndPoint();
        // 30 x 20 KB = ~600 KB: more than one 256 KB Service Bus batch.
        var sent = Enumerable.Range(0, 30).Select(i => Encoding.UTF8.GetBytes($"{i:D2}:" + new string('x', 20_000))).ToList();
        var received = new ConcurrentQueue<string>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.PostBatchAsync(sent, TestContext.Current.CancellationToken);
        await ep.StartListeningAsync((raw, _) =>
        {
            received.Enqueue(Encoding.UTF8.GetString(raw, 0, 2));
            if (received.Count == sent.Count) done.TrySetResult();
            return Task.FromResult(ProcessResult.Completed);
        }, TestContext.Current.CancellationToken);

        await done.Task.WaitAsync(Timeout, TestContext.Current.CancellationToken);
        await ep.StopListeningAsync();

        Assert.Equal(Enumerable.Range(0, 30).Select(i => $"{i:D2}"), received);
    }

    [Fact]
    public async Task PostBatch_MessageLargerThanABatch_Throws_WithSplitHint()
    {
        RequireServiceBus();
        var ep = CreateEndPoint();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            ep.PostBatchAsync([new byte[2_000_000]], TestContext.Current.CancellationToken));

        Assert.Contains("splitThresholdBytes", ex.Message);
    }

    // =====================================================================
    // Broker health check (#53)
    // =====================================================================

    [Fact]
    public async Task BrokerHealth_UnreachableNamespace_IsUnhealthy_AndNamesTheEndpoint()
    {
        var report = await BrokerHealthTestHelper.CheckAsync(b => b
            .AddMemoryEndPoint("Mem")
            .AddAzureServiceBusEndPoint("SbDown",
                new AzureServiceBusSettings { ConnectionString = UnreachableConnectionString, QueueName = "q" }, EndpointMode.WriteOnly));

        Assert.Equal(BrokerHealthStatus.Unhealthy, report.Status);
        Assert.Contains("SbDown", report.Message);
        Assert.Equal(BrokerHealthStatus.Unhealthy, report.Endpoints.Single(e => e.Name == "SbDown").Status);
        Assert.Equal(BrokerHealthStatus.Healthy, report.Endpoints.Single(e => e.Name == "Mem").Status);
    }

    [Fact]
    public async Task BrokerHealth_Emulator_IsHealthy()
    {
        RequireServiceBus();
        // Write-only: the health check peeks the queue without starting a processor on the shared test queue.
        var report = await BrokerHealthTestHelper.CheckAsync(b => b
            .AddAzureServiceBusEndPoint("Sb", new AzureServiceBusSettings { ConnectionString = ConnectionString, QueueName = TestQueue },
                EndpointMode.WriteOnly));

        Assert.Equal(BrokerHealthStatus.Healthy, report.Status);
        Assert.Equal("Sb", Assert.Single(report.Endpoints).Name);
    }

    // =====================================================================
    // helpers
    // =====================================================================

    /// <summary>Nothing listens on port 1, so connections fail fast.</summary>
    private const string UnreachableConnectionString =
        "Endpoint=sb://127.0.0.1:1;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";

    private static void RequireServiceBus()
        => Assert.SkipUnless(!string.IsNullOrWhiteSpace(ConnectionString),
            $"Set {ConnectionStringVariable} to run Azure Service Bus integration tests (scripts/setup-servicebus.ps1).");

    private AzureServiceBusEndPoint CreateEndPoint(CapturingLogger<AzureServiceBusEndPoint>? logger = null,
        bool readDeadLetterQueue = false, bool topic = false, bool subscription = false)
    {
        var settings = new AzureServiceBusSettings
        {
            ConnectionString = ConnectionString,
            QueueName = topic ? null : TestQueue,
            TopicName = topic ? TestTopic : null,
            SubscriptionName = topic && subscription ? TestSubscription : null,
            ReadDeadLetterQueue = readDeadLetterQueue
        };
        var ep = new AzureServiceBusEndPoint("SbTest", settings, logger ?? new CapturingLogger<AzureServiceBusEndPoint>());
        _endpoints.Add(ep);
        return ep;
    }

    private static async Task<ServiceBusReceivedMessage?> PeekAsync(SubQueue subQueue)
    {
        await using var client = new ServiceBusClient(ConnectionString);
        await using var receiver = client.CreateReceiver(TestQueue, new ServiceBusReceiverOptions { SubQueue = subQueue });
        return await receiver.PeekMessageAsync(cancellationToken: TestContext.Current.CancellationToken);
    }

    /// <summary>Receives (and completes) the next message from the test queue's dead-letter queue.</summary>
    private static async Task<ServiceBusReceivedMessage> ReceiveDeadLetterAsync()
    {
        await using var client = new ServiceBusClient(ConnectionString);
        await using var receiver = client.CreateReceiver(TestQueue, new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter });
        var message = await receiver.ReceiveMessageAsync(Timeout, TestContext.Current.CancellationToken)
                      ?? throw new TimeoutException("No message arrived in the dead-letter queue.");
        await receiver.CompleteMessageAsync(message, TestContext.Current.CancellationToken);
        return message;
    }

    /// <summary>
    /// Empties one entity. A single empty receive is not proof it is empty: a fresh receiver on the emulator can take a
    /// while to deliver, and a message still locked by a stopped processor only comes back when its lock (30 s) expires.
    /// So keep receiving until a peek — on a new receiver each time, because a receiver's peek cursor only moves forward —
    /// finds nothing.
    /// </summary>
    private static async Task DrainAsync(Func<ServiceBusReceiver> createReceiver)
    {
        var ct = TestContext.Current.CancellationToken;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(45);
        await using var receiver = createReceiver();
        while (true)
        {
            await using (var peeker = createReceiver())
            {
                if (await peeker.PeekMessageAsync(cancellationToken: ct) is null)
                    return;
            }

            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"Could not drain '{receiver.EntityPath}' before the test.");

            var batch = await receiver.ReceiveMessagesAsync(100, TimeSpan.FromSeconds(2), ct);
            foreach (var m in batch)
                await receiver.CompleteMessageAsync(m, ct);
        }
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Condition was not met in time.");
            await Task.Delay(200, TestContext.Current.CancellationToken);
        }
    }

    public async ValueTask InitializeAsync()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString)) return;

        // Start every integration test from empty entities (the emulator's entities are shared and fixed).
        await using var client = new ServiceBusClient(ConnectionString);
        foreach (var subQueue in new[] { SubQueue.None, SubQueue.DeadLetter })
        {
            var options = new ServiceBusReceiverOptions { SubQueue = subQueue };
            await DrainAsync(() => client.CreateReceiver(TestQueue, options));
            await DrainAsync(() => client.CreateReceiver(TestTopic, TestSubscription, options));
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var ep in _endpoints)
            await ep.DisposeAsync();
    }
}
