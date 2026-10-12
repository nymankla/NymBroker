using System.Text;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;
using NymBroker.Endpoint.AwsSqs;

namespace NymBroker.Tests;

/// <summary>
/// Tests for <see cref="AwsSqsEndPoint"/>. Unit tests always run; integration tests need an SQS-compatible server
/// (ElasticMQ, <c>scripts/setup-sqs.ps1</c>) and are skipped unless <c>NYMBROKER_SQS_URL</c> is set, e.g.
/// <c>http://localhost:9324</c>. Each integration test creates its own queues (and dead-letter queue) and deletes them.
/// </summary>
public sealed class AwsSqsEndPointTests : IAsyncLifetime
{
    private const string ServiceUrlVariable = "NYMBROKER_SQS_URL";
    private const int MaxReceiveCount = 3;
    private static readonly string? ServiceUrl = Environment.GetEnvironmentVariable(ServiceUrlVariable);
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
    private static readonly AWSCredentials LocalCredentials = new BasicAWSCredentials("local", "local");

    private readonly List<AwsSqsEndPoint> _endpoints = [];
    private readonly List<string> _queues = [];
    private AmazonSQSClient? _admin;

    // =====================================================================
    // Unit tests (always run)
    // =====================================================================

    public static TheoryData<string, AwsSqsSettings> InvalidSettings => new()
    {
        { "no queue", new AwsSqsSettings() },
        { "url and name", new AwsSqsSettings { QueueUrl = "http://localhost/1/q", QueueName = "q" } },
        { "relative url", new AwsSqsSettings { QueueUrl = "q" } },
        { "bad service url", new AwsSqsSettings { QueueName = "q", ServiceUrl = "localhost" } },
        { "wait time", new AwsSqsSettings { QueueName = "q", WaitTimeSeconds = 21 } },
        { "zero messages", new AwsSqsSettings { QueueName = "q", MaxNumberOfMessages = 0 } },
        { "eleven messages", new AwsSqsSettings { QueueName = "q", MaxNumberOfMessages = 11 } },
        { "visibility", new AwsSqsSettings { QueueName = "q", VisibilityTimeout = TimeSpan.FromHours(13) } },
        { "retry delay", new AwsSqsSettings { QueueName = "q", RetryDelay = TimeSpan.FromSeconds(-1) } },
        { "zero concurrency", new AwsSqsSettings { QueueName = "q", MaxConcurrentCalls = 0 } },
        { "group on standard queue", new AwsSqsSettings { QueueName = "q", MessageGroupId = "g" } },
    };

    [Theory]
    [MemberData(nameof(InvalidSettings))]
    public void Settings_Invalid_Throw(string _, AwsSqsSettings settings)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => settings.Validate("Sqs"));
        Assert.Contains("'Sqs'", ex.Message);
    }

    [Theory]
    [InlineData("orders", null, false)]
    [InlineData("orders.fifo", null, true)]
    [InlineData(null, "https://sqs.eu-north-1.amazonaws.com/123456789012/orders.fifo", true)]
    [InlineData(null, "https://sqs.eu-north-1.amazonaws.com/123456789012/orders", false)]
    public void Settings_DetectFifoQueues(string? name, string? url, bool fifo)
    {
        var settings = new AwsSqsSettings { QueueName = name, QueueUrl = url, MessageGroupId = fifo ? "g" : null };
        settings.Validate("Sqs");
        Assert.Equal(fifo, settings.IsFifo);
    }

    [Fact]
    public void Encode_Envelope_IsSentAsTextUnchanged()
    {
        var bytes = Encoding.UTF8.GetBytes("""{"id":"1","message":{"text":"åäö 😀"}}""");

        var (body, isBase64) = SqsWireFormat.Encode(bytes);

        Assert.False(isBase64);
        Assert.Equal(bytes, Encoding.UTF8.GetBytes(body));
        Assert.Null(SqsWireFormat.Attributes(isBase64));
    }

    [Theory]
    [InlineData(new byte[] { 0xFF, 0xFE, 0x00, 0x80 })]   // not UTF-8
    [InlineData(new byte[] { (byte)'a', 0x00, (byte)'b' })] // UTF-8, but NUL is not allowed in an SQS body
    [InlineData(new byte[] { 0xEF, 0xBF, 0xBF })]           // UTF-8 for U+FFFF, not allowed
    public void Encode_BytesSqsCannotCarry_AreBase64_AndRoundTrip(byte[] bytes)
    {
        var (body, isBase64) = SqsWireFormat.Encode(bytes);

        Assert.True(isBase64);
        var message = new Message { Body = body, MessageAttributes = SqsWireFormat.Attributes(isBase64) };
        Assert.Equal(bytes, SqsWireFormat.Decode(message));
    }

    [Fact]
    public void Decode_WithoutAttributes_IsUtf8()
        => Assert.Equal("plain"u8.ToArray(), SqsWireFormat.Decode(new Message { Body = "plain" }));

    [Fact]
    public void FifoIds_ComeFromTheEnvelope()
    {
        var id = Guid.NewGuid();
        var correlation = Guid.NewGuid();
        var bytes = Encoding.UTF8.GetBytes($$"""{"message":{"id":"inner"},"id":"{{id}}","correlationId":"{{correlation}}"}""");

        Assert.Equal((correlation.ToString(), id.ToString()), SqsWireFormat.FifoIds(bytes, null));
        Assert.Equal(("orders", id.ToString()), SqsWireFormat.FifoIds(bytes, "orders"));
    }

    [Theory]
    [InlineData("not json at all")]
    [InlineData("""{"id":"00000000-0000-0000-0000-000000000000"}""")]
    [InlineData("[1,2,3]")]
    public void FifoIds_WithoutAnEnvelopeId_UseTheDefaultGroupAndAContentHash(string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text);

        var (group, dedup) = SqsWireFormat.FifoIds(bytes, null);

        Assert.Equal(SqsWireFormat.DefaultMessageGroupId, group);
        Assert.Equal(64, dedup.Length);
        Assert.Equal(dedup, SqsWireFormat.FifoIds(bytes, null).DeduplicationId);
    }

    [Fact]
    public void Chunk_KeepsOrder_TenEntriesPerRequest()
    {
        var messages = Enumerable.Range(0, 25).Select(i => Encoding.UTF8.GetBytes(i.ToString())).ToList();

        var chunks = SqsWireFormat.Chunk(messages).ToList();

        Assert.Equal([10, 10, 5], chunks.Select(c => c.Count));
        Assert.Equal(messages, chunks.SelectMany(c => c.Select(e => e.Original)));
    }

    [Fact]
    public void Chunk_RespectsTheRequestSizeLimit()
    {
        var big = new byte[100 * 1024];
        Array.Fill(big, (byte)'x');
        var messages = Enumerable.Repeat(big, 5).ToList();

        var chunks = SqsWireFormat.Chunk(messages).ToList();

        Assert.Equal([2, 2, 1], chunks.Select(c => c.Count));
    }

    [Fact]
    public async Task UsesNativeDeadLetter_IsFalse()
    {
        await using var ep = new AwsSqsEndPoint("Sqs", UnreachableSettings(), new CapturingLogger<AwsSqsEndPoint>());
        Assert.False(((IEndPointEventDriven)ep).UsesNativeDeadLetter);
    }

    [Fact]
    public async Task HealthCheck_UnreachableServer_ReturnsUnhealthyWithoutThrowing()
    {
        var logger = new CapturingLogger<AwsSqsEndPoint>();
        await using var ep = new AwsSqsEndPoint("Sqs", UnreachableSettings(), logger);

        var result = ep.HealthCheck();

        Assert.False(result.IsHealthy);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error);
    }

    [Fact]
    public async Task AddAwsSqsEndPoint_RegistersEndpoint()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker().AddAwsSqsEndPoint("Sqs", UnreachableSettings()).Build();

        await using var sp = services.BuildServiceProvider();

        Assert.IsType<AwsSqsEndPoint>(sp.GetRequiredKeyedService<IEndPoint>("Sqs"));
    }

    [Fact]
    public void AddAwsSqsEndPoint_InvalidSettings_ThrowsAtRegistration()
    {
        var builder = new ServiceCollection().AddNymBroker();
        Assert.Throws<InvalidOperationException>(() => builder.AddAwsSqsEndPoint("Sqs", new AwsSqsSettings()));
    }

    [Fact]
    public async Task WithAwsSqs_RegistersConfiguredEndpoint_CaseInsensitively()
    {
        const string json = """
            {
              "NymBroker": {
                "Endpoints": [
                  { "name": "Sqs1", "type": "awssqs",
                    "config": { "queueName": "orders", "region": "eu-north-1", "maxNumberOfMessages": 5, "retryDelay": "00:00:10" } }
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
            services.AddNymBroker().LoadConfiguration(path).WithAwsSqs().Build();

            await using var sp = services.BuildServiceProvider();

            Assert.IsType<AwsSqsEndPoint>(sp.GetRequiredKeyedService<IEndPoint>("Sqs1"));
        }
        finally
        {
            File.Delete(path);
        }
    }

    // =====================================================================
    // Integration tests (ElasticMQ)
    // =====================================================================

    [Fact]
    public async Task PostedMessage_ReachesHandler_WithBytesUnchanged_AndIsDeleted()
    {
        var queue = await CreateQueueAsync();
        var ep = CreateEndPoint(queue);
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var payload = Encoding.UTF8.GetBytes("""{"id":"1","message":{"text":"åäö"}}""");

        await ep.StartListeningAsync((raw, _) => { received.TrySetResult(raw); return Task.FromResult(ProcessResult.Completed); }, Ct);
        await ep.PostAsync(payload, Ct);

        Assert.Equal(payload, await received.Task.WaitAsync(Timeout, Ct));
        await ep.StopListeningAsync();
        Assert.Equal(0, await CountAsync(queue.Url));
    }

    [Fact]
    public async Task BinaryPayload_RoundTrips()
    {
        var queue = await CreateQueueAsync();
        var ep = CreateEndPoint(queue);
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        byte[] payload = [0x00, 0xFF, 0x10, 0x80, 0x7F];

        await ep.StartListeningAsync((raw, _) => { received.TrySetResult(raw); return Task.FromResult(ProcessResult.Completed); }, Ct);
        await ep.PostAsync(payload, Ct);

        Assert.Equal(payload, await received.Task.WaitAsync(Timeout, Ct));
    }

    [Fact]
    public async Task RetryResult_IsRedelivered_UntilMaxReceiveCount_ThenMovedToTheDeadLetterQueueByRedrive()
    {
        var queue = await CreateQueueAsync();
        var logger = new CapturingLogger<AwsSqsEndPoint>();
        var ep = CreateEndPoint(queue, logger: logger);
        var calls = 0;

        await ep.StartListeningAsync((_, _) =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(ProcessResult.Retry(new InvalidOperationException("transient")));
        }, Ct);
        await ep.PostAsync("retry-me"u8.ToArray(), Ct);

        var deadLettered = await ReceiveOneAsync(queue.DeadLetterUrl!);
        await ep.StopListeningAsync();

        Assert.Equal("retry-me", deadLettered.Body);
        Assert.Equal(MaxReceiveCount, Volatile.Read(ref calls));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("redelivered"));
    }

    [Fact]
    public async Task ThrowingHandler_CountsAsRetry_AndTheRedeliveryIsProcessed()
    {
        var queue = await CreateQueueAsync();
        var logger = new CapturingLogger<AwsSqsEndPoint>();
        var ep = CreateEndPoint(queue, logger: logger);
        var calls = 0;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.StartListeningAsync((_, _) =>
        {
            if (Interlocked.Increment(ref calls) == 1)
                throw new InvalidOperationException("boom");
            done.TrySetResult();
            return Task.FromResult(ProcessResult.Completed);
        }, Ct);
        await ep.PostAsync("throw-once"u8.ToArray(), Ct);

        await done.Task.WaitAsync(Timeout, Ct);
        await ep.StopListeningAsync();

        Assert.Equal(2, Volatile.Read(ref calls));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error && e.Message.Contains("Unhandled error dispatching"));
        Assert.Equal(0, await CountAsync(queue.Url));
    }

    [Fact]
    public async Task AfterStopListening_HandlerIsNeverCalled_AndMessageStaysQueued()
    {
        var queue = await CreateQueueAsync();
        var ep = CreateEndPoint(queue, waitTimeSeconds: 1);
        var calls = 0;
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.StartListeningAsync((_, _) =>
        {
            Interlocked.Increment(ref calls);
            first.TrySetResult();
            return Task.FromResult(ProcessResult.Completed);
        }, Ct);
        await ep.PostAsync("before-stop"u8.ToArray(), Ct);
        await first.Task.WaitAsync(Timeout, Ct);

        await ep.StopListeningAsync();
        await ep.PostAsync("after-stop"u8.ToArray(), Ct);
        await Task.Delay(1500, Ct);

        Assert.Equal(1, Volatile.Read(ref calls));
        Assert.Equal("after-stop", (await ReceiveOneAsync(queue.Url)).Body);
    }

    [Fact]
    public async Task StopListening_WhileHandlerRuns_WaitsAndDeletesTheMessage()
    {
        var queue = await CreateQueueAsync();
        var ep = CreateEndPoint(queue);
        var calls = 0;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.StartListeningAsync(async (_, _) =>
        {
            Interlocked.Increment(ref calls);
            started.TrySetResult();
            await release.Task;   // ignores the stop token, like a handler finishing its work
            return ProcessResult.Completed;
        }, Ct);
        await ep.PostAsync("in-flight"u8.ToArray(), Ct);
        await started.Task.WaitAsync(Timeout, Ct);

        var stop = ep.StopListeningAsync();
        await Task.Delay(300, Ct);
        Assert.False(stop.IsCompleted);   // waits for the handler
        release.SetResult();
        await stop.WaitAsync(Timeout, Ct);

        Assert.Equal(1, Volatile.Read(ref calls));
        Assert.Equal(0, await CountAsync(queue.Url));   // settled although stopping
    }

    [Fact]
    public async Task StopListening_ReleasesMessagesOfTheBatchThatWereNotHandled()
    {
        var queue = await CreateQueueAsync(visibilityTimeoutSeconds: 300);
        var ep = CreateEndPoint(queue);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.PostBatchAsync([.. Enumerable.Range(0, 5).Select(i => Encoding.UTF8.GetBytes($"m{i}"))], Ct);
        await ep.StartListeningAsync(async (_, ct) =>
        {
            started.TrySetResult();
            await Task.Delay(System.Threading.Timeout.Infinite, ct);   // honours the stop
            return ProcessResult.Completed;
        }, Ct);
        await started.Task.WaitAsync(Timeout, Ct);
        await ep.StopListeningAsync();

        // Visible again at once, not after the 300 s visibility timeout.
        var visible = await _admin!.GetQueueAttributesAsync(new GetQueueAttributesRequest(queue.Url, ["ApproximateNumberOfMessages"]), Ct);
        Assert.True(visible.ApproximateNumberOfMessages >= 4, $"visible: {visible.ApproximateNumberOfMessages}");
    }

    [Fact]
    public async Task PostBatch_ToFifoQueue_ArrivesInOrder()
    {
        var queue = await CreateQueueAsync(fifo: true);
        var ep = CreateEndPoint(queue, messageGroupId: "one-group");
        var messages = Enumerable.Range(0, 25).Select(i => Encoding.UTF8.GetBytes($$"""{"id":"{{Guid.NewGuid()}}","n":{{i}}}""")).ToList();
        var received = new List<byte[]>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.PostBatchAsync(messages, Ct);
        await ep.StartListeningAsync((raw, _) =>
        {
            lock (received)
            {
                received.Add(raw);
                if (received.Count == messages.Count) done.TrySetResult();
            }
            return Task.FromResult(ProcessResult.Completed);
        }, Ct);

        await done.Task.WaitAsync(Timeout, Ct);
        Assert.Equal(messages, received);
    }

    [Fact]
    public async Task Fifo_SameEnvelopeTwice_IsDeliveredOnce()
    {
        var queue = await CreateQueueAsync(fifo: true);
        var ep = CreateEndPoint(queue, waitTimeSeconds: 1);
        var envelope = Encoding.UTF8.GetBytes($$"""{"id":"{{Guid.NewGuid()}}","correlationId":"{{Guid.NewGuid()}}","message":{"x":1} }""");
        var calls = 0;

        await ep.PostAsync(envelope, Ct);
        await ep.PostAsync(envelope, Ct);   // a producer retry: same message id
        await ep.StartListeningAsync((_, _) => { Interlocked.Increment(ref calls); return Task.FromResult(ProcessResult.Completed); }, Ct);
        await Task.Delay(2500, Ct);

        Assert.Equal(1, Volatile.Read(ref calls));
    }

    [Fact]
    public async Task ThroughBroker_ConsumerFailure_GoesToTheBrokersDeadLetterEndpoint_AndIsDeleted()
    {
        var queue = await CreateQueueAsync();
        Interlocked.Exchange(ref FailingSqsOrderConsumer.Calls, 0);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .AddAwsSqsEndPoint("Sqs", Settings(queue))
            .AddMemoryEndPoint("DLQ", mode: EndpointMode.WriteOnly)
            .WithDeadLetterEndpoint("DLQ")
            .AddConsumer<FailingSqsOrderConsumer>()
            .Build();
        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<INymBroker>();
        var brokerDlq = (MemoryQueueEndPoint)sp.GetRequiredKeyedService<IEndPoint>("DLQ");

        await broker.StartAsync(Ct);
        await broker.PostAsync("Sqs", new SqsOrder { Id = 7 }, Ct);
        string? deadLettered = null;
        var deadline = DateTime.UtcNow + Timeout;
        while (deadLettered is null && DateTime.UtcNow < deadline)
        {
            await foreach (var item in brokerDlq.ReadAsync(Ct)) deadLettered = item;
            if (deadLettered is null) await Task.Delay(100, Ct);
        }
        await broker.StopAsync(Ct);

        Assert.NotNull(deadLettered);
        Assert.Contains(DeadLetterReasons.ConsumerFailed, deadLettered);
        Assert.Equal(1, Volatile.Read(ref FailingSqsOrderConsumer.Calls));
        Assert.Equal(0, await CountAsync(queue.Url));
        Assert.Null((await _admin!.ReceiveMessageAsync(new ReceiveMessageRequest(queue.DeadLetterUrl) { WaitTimeSeconds = 0 }, Ct)).Messages?.FirstOrDefault());
    }

    [Fact]
    public async Task QueueName_IsResolvedToItsUrl()
    {
        var queue = await CreateQueueAsync();
        var ep = CreateEndPoint(queue, byName: true);
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);

        await ep.StartListeningAsync((raw, _) => { received.TrySetResult(raw); return Task.FromResult(ProcessResult.Completed); }, Ct);
        await ep.PostAsync("by-name"u8.ToArray(), Ct);

        Assert.Equal("by-name"u8.ToArray(), await received.Task.WaitAsync(Timeout, Ct));
    }

    [Fact]
    public async Task BrokerHealth_ElasticMq_IsHealthy()
    {
        var queue = await CreateQueueAsync();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker().AddAwsSqsEndPoint("Sqs", Settings(queue), EndpointMode.WriteOnly).Build();
        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<INymBroker>();

        await broker.StartAsync(Ct);
        var report = await broker.CheckHealthAsync(Ct);
        await broker.StopAsync(Ct);

        Assert.Equal(BrokerHealthStatus.Healthy, report.Status);
    }

    // =====================================================================
    // helpers
    // =====================================================================

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private sealed record TestQueue(string Name, string Url, string? DeadLetterUrl);

    /// <summary>Nothing listens on port 1, so connections fail fast.</summary>
    private static AwsSqsSettings UnreachableSettings()
        => new() { QueueUrl = "http://127.0.0.1:1/000000000000/q", ServiceUrl = "http://127.0.0.1:1", Credentials = LocalCredentials };

    private static AwsSqsSettings Settings(TestQueue queue, bool byName = false, int waitTimeSeconds = 1, string? messageGroupId = null) => new()
    {
        QueueUrl = byName ? null : queue.Url,
        QueueName = byName ? queue.Name : null,
        ServiceUrl = ServiceUrl,
        Credentials = LocalCredentials,
        WaitTimeSeconds = waitTimeSeconds,
        MessageGroupId = messageGroupId
    };

    private AwsSqsEndPoint CreateEndPoint(TestQueue queue, CapturingLogger<AwsSqsEndPoint>? logger = null, bool byName = false,
        int waitTimeSeconds = 1, string? messageGroupId = null)
    {
        var ep = new AwsSqsEndPoint("SqsTest", Settings(queue, byName, waitTimeSeconds, messageGroupId), logger ?? new CapturingLogger<AwsSqsEndPoint>());
        _endpoints.Add(ep);
        return ep;
    }

    /// <summary>A unique queue with a dead-letter queue (redrive after <see cref="MaxReceiveCount"/> receives).</summary>
    private async Task<TestQueue> CreateQueueAsync(bool fifo = false, int visibilityTimeoutSeconds = 30)
    {
        Assert.SkipUnless(!string.IsNullOrWhiteSpace(ServiceUrl),
            $"Set {ServiceUrlVariable} to run AWS SQS integration tests (scripts/setup-sqs.ps1).");
        _admin ??= new AmazonSQSClient(LocalCredentials, new AmazonSQSConfig { ServiceURL = ServiceUrl, AuthenticationRegion = "us-east-1" });

        var suffix = fifo ? ".fifo" : "";
        var name = $"nymbroker-test-{Guid.NewGuid():N}";
        var fifoAttributes = fifo ? new Dictionary<string, string> { ["FifoQueue"] = "true" } : [];

        var dlqUrl = (await _admin.CreateQueueAsync(new CreateQueueRequest($"{name}-dlq{suffix}") { Attributes = new(fifoAttributes) }, Ct)).QueueUrl;
        _queues.Add(dlqUrl);
        var dlqArn = (await _admin.GetQueueAttributesAsync(new GetQueueAttributesRequest(dlqUrl, ["QueueArn"]), Ct)).QueueARN;

        var attributes = new Dictionary<string, string>(fifoAttributes)
        {
            ["VisibilityTimeout"] = visibilityTimeoutSeconds.ToString(),
            ["RedrivePolicy"] = $$"""{"maxReceiveCount":"{{MaxReceiveCount}}","deadLetterTargetArn":"{{dlqArn}}"}"""
        };
        var url = (await _admin.CreateQueueAsync(new CreateQueueRequest(name + suffix) { Attributes = attributes }, Ct)).QueueUrl;
        _queues.Add(url);
        return new TestQueue(name + suffix, url, dlqUrl);
    }

    /// <summary>Visible + in-flight messages.</summary>
    private async Task<int> CountAsync(string queueUrl)
    {
        var attributes = await _admin!.GetQueueAttributesAsync(
            new GetQueueAttributesRequest(queueUrl, ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]), Ct);
        return attributes.ApproximateNumberOfMessages + attributes.ApproximateNumberOfMessagesNotVisible;
    }

    private async Task<Message> ReceiveOneAsync(string queueUrl)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (DateTime.UtcNow < deadline)
        {
            var response = await _admin!.ReceiveMessageAsync(new ReceiveMessageRequest(queueUrl) { WaitTimeSeconds = 1 }, Ct);
            if (response.Messages is [var message, ..])
            {
                await _admin.DeleteMessageAsync(queueUrl, message.ReceiptHandle, Ct);
                return message;
            }
        }
        throw new TimeoutException($"No message arrived in '{queueUrl}'.");
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var ep in _endpoints)
            await ep.DisposeAsync();
        if (_admin is null) return;
        foreach (var url in _queues)
        {
            try { await _admin.DeleteQueueAsync(url); }
            catch (QueueDoesNotExistException) { }
        }
        _admin.Dispose();
    }
}

[MessageName("tests.sqs-order")]
public sealed class SqsOrder
{
    public int Id { get; set; }
}

public sealed class FailingSqsOrderConsumer : IConsume<SqsOrder>
{
    public static int Calls;

    public Task ConsumeAsync(SqsOrder message, IMessageContext context, CancellationToken ct = default)
    {
        Interlocked.Increment(ref Calls);
        throw new InvalidOperationException("consumer failed");
    }
}
