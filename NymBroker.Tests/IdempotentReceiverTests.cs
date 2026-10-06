using System.Diagnostics.Metrics;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Consume;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Idempotency;
using NymBroker.Core.PubSub;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace NymBroker.Tests;

/// <summary>The idempotent receiver: claim → process → complete / release (#55).</summary>
public sealed class IdempotentReceiverTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private sealed class Ping { public int Id { get; set; } public string Text { get; set; } = ""; }

    private sealed class PingConsumer : IConsume<Ping>
    {
        private int _failuresLeft;
        public List<Ping> Received { get; } = [];
        public int Calls;

        public void FailNext(int times) => _failuresLeft = times;

        public Task ConsumeAsync(Ping message, IMessageContext context, CancellationToken ct = default)
        {
            Interlocked.Increment(ref Calls);
            if (Interlocked.Decrement(ref _failuresLeft) >= 0)
                throw new InvalidOperationException("consumer failed");
            lock (Received) Received.Add(message);
            return Task.CompletedTask;
        }
    }

    /// <summary>An endpoint whose transport redelivers and dead-letters itself (like RabbitMQ / Service Bus).</summary>
    private sealed class NativeEndPoint : IEndPointEventDriven
    {
        public EndpointMode Mode => EndpointMode.ReadWrite;
        public bool UsesNativeDeadLetter => true;
        public Task PostAsync(byte[] message, CancellationToken ct = default) => Task.CompletedTask;
        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
        public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct) => Task.CompletedTask;
        public Task StopListeningAsync() => Task.CompletedTask;
    }

    private sealed class ThrowingStore(bool throwOnClaim, bool throwOnSettle) : IIdempotencyStore
    {
        private readonly InMemoryIdempotencyStore _inner = new(TimeSpan.FromHours(1));

        public ValueTask<IdempotencyClaimResult> TryClaimAsync(Guid messageId, CancellationToken ct = default)
            => throwOnClaim ? throw new InvalidOperationException("store down") : _inner.TryClaimAsync(messageId, ct);

        public ValueTask CompleteAsync(Guid messageId, CancellationToken ct = default)
            => throwOnSettle ? throw new InvalidOperationException("store down") : _inner.CompleteAsync(messageId, ct);

        public ValueTask ReleaseAsync(Guid messageId, CancellationToken ct = default)
            => throwOnSettle ? throw new InvalidOperationException("store down") : _inner.ReleaseAsync(messageId, ct);
    }

    private sealed record Setup(NymBrokerImpl Broker, PingConsumer Consumer, MessageSerializerJson Serializer, IIdempotencyStore Store);

    private static Setup BuildBroker(TimeSpan? ttl = null, IIdempotencyStore? store = null, ILogger<NymBrokerImpl>? logger = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var consumer = new PingConsumer();
        services.AddKeyedSingleton<IMessageConsumer>(nameof(PingConsumer), consumer);
        var sp = services.BuildServiceProvider();
        var serializer = new MessageSerializerJson();

        var broker = new NymBrokerImpl(
            serializer,
            new AggregatorImpl(NullLogger<AggregatorImpl>.Instance),
            new MessageTypeRegistry(),
            new ConsumerDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<ConsumerDispatcher>.Instance),
            new SubscriberDispatcher(sp.GetRequiredService<IServiceScopeFactory>(), NullLogger<SubscriberDispatcher>.Instance),
            logger ?? NullLogger<NymBrokerImpl>.Instance);

        broker.RegisterConsumer(typeof(Ping), nameof(PingConsumer));
        broker.AddEndpoint("Native", new NativeEndPoint());

        store ??= new InMemoryIdempotencyStore(ttl ?? TimeSpan.FromHours(1));
        broker.SetIdempotencyStore(store);

        return new Setup(broker, consumer, serializer, store);
    }

    private static string Serialize<T>(MessageSerializerJson serializer, T message) where T : class
        => Serialize(serializer, new MessageContext<T> { Message = message });

    private static string Serialize<T>(MessageSerializerJson serializer, MessageContext<T> context) where T : class
    {
        using var stream = serializer.Serialize(context);
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    private static Guid IdOf(MessageSerializerJson serializer, string json)
        => serializer.Deserialize(System.Text.Encoding.UTF8.GetBytes(json)).Id;

    // --- Basics ---

    [Fact]
    public async Task SameMessageTwice_ConsumerCalledOnlyOnce()
    {
        var s = BuildBroker();
        var json = Serialize(s.Serializer, new Ping { Id = 1 });

        Assert.Equal(ProcessOutcome.Completed, (await s.Broker.ProcessAsync(json, "In", Ct)).Outcome);
        Assert.Equal(ProcessOutcome.Completed, (await s.Broker.ProcessAsync(json, "In", Ct)).Outcome);

        Assert.Single(s.Consumer.Received);
    }

    [Fact]
    public async Task DifferentMessages_BothDelivered()
    {
        var s = BuildBroker();

        await s.Broker.ProcessAsync(Serialize(s.Serializer, new Ping { Id = 1 }), "In", Ct);
        await s.Broker.ProcessAsync(Serialize(s.Serializer, new Ping { Id = 2 }), "In", Ct);

        Assert.Equal(2, s.Consumer.Received.Count);
    }

    [Fact]
    public async Task ExpiredEntry_AllowsReprocessing()
    {
        var s = BuildBroker(ttl: TimeSpan.FromMilliseconds(1));
        var json = Serialize(s.Serializer, new Ping { Id = 99 });

        await s.Broker.ProcessAsync(json, "In", Ct);
        await Task.Delay(20, Ct);
        await s.Broker.ProcessAsync(json, "In", Ct);

        Assert.Equal(2, s.Consumer.Received.Count);
    }

    // --- The retry bug: a message marked seen before processing was lost on redelivery ---

    [Fact]
    public async Task ConsumerFailure_WithNativeRetry_ReleasesClaim_AndRedeliveryIsProcessed()
    {
        var s = BuildBroker();
        s.Consumer.FailNext(1);
        var json = Serialize(s.Serializer, new Ping { Id = 7 });

        var first = await s.Broker.ProcessAsync(json, "Native", Ct);
        var redelivery = await s.Broker.ProcessAsync(json, "Native", Ct);
        var duplicate = await s.Broker.ProcessAsync(json, "Native", Ct);

        Assert.Equal(ProcessOutcome.Retry, first.Outcome);
        Assert.Equal(ProcessOutcome.Completed, redelivery.Outcome);
        Assert.Equal(ProcessOutcome.Completed, duplicate.Outcome);
        Assert.Equal(7, Assert.Single(s.Consumer.Received).Id);   // processed exactly once, on the redelivery
        Assert.Equal(2, s.Consumer.Calls);                        // the duplicate never reached the consumer
    }

    [Fact]
    public async Task ConsumerFailure_DeadLetteredByBroker_CompletesClaim_AndRedeliveryIsDropped()
    {
        // A non-native source: the broker handles the failure itself (dead-letter endpoint) and reports Completed.
        var s = BuildBroker();
        s.Consumer.FailNext(1);
        var json = Serialize(s.Serializer, new Ping { Id = 8 });

        Assert.Equal(ProcessOutcome.Completed, (await s.Broker.ProcessAsync(json, "In", Ct)).Outcome);
        Assert.Equal(ProcessOutcome.Completed, (await s.Broker.ProcessAsync(json, "In", Ct)).Outcome);

        Assert.Equal(1, s.Consumer.Calls);
        Assert.Empty(s.Consumer.Received);
    }

    [Fact]
    public async Task RouteDestinationThrows_ReleasesClaim_AndRetryIsProcessed()
    {
        var s = BuildBroker();
        var failing = new FlakyEndPoint(failures: 1);
        s.Broker.AddEndpoint("Out", failing);
        s.Broker.Route<Ping>().To("Out").Build();
        var json = Serialize(s.Serializer, new Ping { Id = 9 });

        Assert.Equal(ProcessOutcome.Retry, (await s.Broker.ProcessAsync(json, "Native", Ct)).Outcome);
        Assert.Equal(ProcessOutcome.Completed, (await s.Broker.ProcessAsync(json, "Native", Ct)).Outcome);
        Assert.Equal(ProcessOutcome.Completed, (await s.Broker.ProcessAsync(json, "Native", Ct)).Outcome);

        Assert.Equal(1, failing.Delivered);
    }

    private sealed class FlakyEndPoint(int failures) : IEndPoint
    {
        private int _failuresLeft = failures;
        public int Delivered;
        public EndpointMode Mode => EndpointMode.WriteOnly;
        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();

        public Task PostAsync(byte[] message, CancellationToken ct = default)
        {
            if (Interlocked.Decrement(ref _failuresLeft) >= 0) throw new IOException("destination down");
            Interlocked.Increment(ref Delivered);
            return Task.CompletedTask;
        }
    }

    // --- Claims held elsewhere, store failures ---

    [Fact]
    public async Task LiveClaimHeldElsewhere_ReturnsRetry_WithoutProcessing()
    {
        var s = BuildBroker();
        var json = Serialize(s.Serializer, new Ping { Id = 10 });
        Assert.Equal(IdempotencyClaimResult.Claimed, await s.Store.TryClaimAsync(IdOf(s.Serializer, json), Ct));   // "another instance"

        var result = await s.Broker.ProcessAsync(json, "Native", Ct);

        Assert.Equal(ProcessOutcome.Retry, result.Outcome);
        Assert.Equal(0, s.Consumer.Calls);
    }

    [Fact]
    public async Task StoreFailsToClaim_ReturnsRetry_AndLogsError_WithoutProcessing()
    {
        var logger = new CapturingLogger<NymBrokerImpl>();
        var s = BuildBroker(store: new ThrowingStore(throwOnClaim: true, throwOnSettle: false), logger: logger);

        var result = await s.Broker.ProcessAsync(Serialize(s.Serializer, new Ping { Id = 11 }), "Native", Ct);

        Assert.Equal(ProcessOutcome.Retry, result.Outcome);
        Assert.Equal(0, s.Consumer.Calls);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error && e.Message.Contains("claim"));
    }

    [Fact]
    public async Task StoreFailsToComplete_KeepsResult_AndLogsError()
    {
        var logger = new CapturingLogger<NymBrokerImpl>();
        var s = BuildBroker(store: new ThrowingStore(throwOnClaim: false, throwOnSettle: true), logger: logger);

        var result = await s.Broker.ProcessAsync(Serialize(s.Serializer, new Ping { Id = 12 }), "Native", Ct);

        Assert.Equal(ProcessOutcome.Completed, result.Outcome);
        Assert.Single(s.Consumer.Received);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error && e.Message.Contains("complete"));
    }

    // --- Split messages, publish, metrics ---

    [Fact]
    public async Task SplitMessage_IsClaimedOnceByItsOwnId_NotPerPart()
    {
        var s = BuildBroker();
        var memory = new MemoryQueueEndPoint("Parts");
        s.Broker.AddEndpoint("Parts", memory);
        var big = new Ping { Id = 13, Text = string.Concat(Enumerable.Range(0, 2000).Select(i => (char)('a' + i * 7 % 26))) };

        await s.Broker.PostAsync("Parts", big, Ct, splitThresholdBytes: 512, compress: false);
        var parts = new List<string>();
        await foreach (var part in memory.ReadAsync(Ct)) parts.Add(part);
        Assert.True(parts.Count > 1);

        foreach (var part in parts) await s.Broker.ProcessAsync(part, "Native", Ct);
        foreach (var part in parts) await s.Broker.ProcessAsync(part, "Native", Ct);   // the whole group redelivered

        Assert.Equal(13, Assert.Single(s.Consumer.Received).Id);
    }

    [Fact]
    public async Task Publish_WithIdempotentReceiver_DeliversEachPublish()
    {
        var s = BuildBroker();

        await s.Broker.PublishAsync(new Ping { Id = 1 }, Ct);
        await s.Broker.PublishAsync(new Ping { Id = 1 }, Ct);   // a new envelope (new ID) each time

        Assert.Equal(2, s.Consumer.Received.Count);
    }

    [Fact]
    public async Task Duplicate_IsCountedInDuplicatesMetric_WithSource()
    {
        var source = $"dup-{Guid.NewGuid():N}";
        var measurements = new List<long>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == NymBrokerDiagnostics.InstrumentationName && instrument.Name == "nymbroker.messages.duplicates")
                    l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            foreach (var tag in tags)
                if (tag.Key == "source" && Equals(tag.Value, source))
                    lock (measurements) measurements.Add(value);
        });
        listener.Start();

        var s = BuildBroker();
        var json = Serialize(s.Serializer, new Ping { Id = 14 });
        await s.Broker.ProcessAsync(json, source, Ct);
        await s.Broker.ProcessAsync(json, source, Ct);
        await s.Broker.ProcessAsync(json, source, Ct);

        lock (measurements) Assert.Equal([1L, 1L], measurements);
    }

    // --- Builder ---

    [Fact]
    public async Task Builder_AddIdempotentReceiver_DropsDuplicates()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker()
            .AddMemoryEndPoint("In")
            .AddConsumer<CountingConsumer>()
            .AddIdempotentReceiver(TimeSpan.FromMinutes(5))
            .Build();
        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<NymBrokerImpl>();
        var json = Serialize(sp.GetRequiredService<MessageSerializerJson>(), new Counted());

        await broker.ProcessAsync(json, "In", Ct);
        await broker.ProcessAsync(json, "In", Ct);

        Assert.Equal(1, CountingConsumer.Count(json));
    }

    [Fact]
    public async Task Builder_CustomStore_IsUsed()
    {
        var store = new InMemoryIdempotencyStore(TimeSpan.FromMinutes(5));
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNymBroker().AddMemoryEndPoint("In").AddIdempotentReceiver(store).Build();
        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<NymBrokerImpl>();
        var json = Serialize(sp.GetRequiredService<MessageSerializerJson>(), new Counted());

        await broker.ProcessAsync(json, "In", Ct);

        Assert.Equal(IdempotencyClaimResult.Duplicate, await store.TryClaimAsync(IdOf(sp.GetRequiredService<MessageSerializerJson>(), json), Ct));
    }

    [MessageName("tests.idempotency.counted")]
    public sealed class Counted { public Guid Marker { get; set; } = Guid.NewGuid(); }

    public sealed class CountingConsumer : IConsume<Counted>
    {
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<Guid, int> Seen = new();

        public static int Count(string json) => Seen.GetValueOrDefault(
            System.Text.Json.JsonDocument.Parse(json).RootElement.GetProperty("message").GetProperty("marker").GetGuid());

        public Task ConsumeAsync(Counted message, IMessageContext context, CancellationToken ct = default)
        {
            Seen.AddOrUpdate(message.Marker, 1, (_, n) => n + 1);
            return Task.CompletedTask;
        }
    }
}

/// <summary>Unit tests of <see cref="InMemoryIdempotencyStore"/>'s claim states.</summary>
public sealed class InMemoryIdempotencyStoreTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Claim_Complete_ThenDuplicate()
    {
        var store = new InMemoryIdempotencyStore(TimeSpan.FromHours(1));
        var id = Guid.NewGuid();

        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(id, Ct));
        Assert.Equal(IdempotencyClaimResult.InProgress, await store.TryClaimAsync(id, Ct));
        await store.CompleteAsync(id, Ct);
        Assert.Equal(IdempotencyClaimResult.Duplicate, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task Release_AllowsClaimAgain_ButNeverForgetsACompletedMessage()
    {
        var store = new InMemoryIdempotencyStore(TimeSpan.FromHours(1));
        var id = Guid.NewGuid();

        await store.TryClaimAsync(id, Ct);
        await store.ReleaseAsync(id, Ct);
        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(id, Ct));

        await store.CompleteAsync(id, Ct);
        await store.ReleaseAsync(id, Ct);
        Assert.Equal(IdempotencyClaimResult.Duplicate, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task ExpiredLease_CanBeTakenOver()
    {
        var store = new InMemoryIdempotencyStore(TimeSpan.FromHours(1), leaseTimeout: TimeSpan.FromMilliseconds(1));
        var id = Guid.NewGuid();

        await store.TryClaimAsync(id, Ct);
        await Task.Delay(20, Ct);

        Assert.Equal(IdempotencyClaimResult.Claimed, await store.TryClaimAsync(id, Ct));
    }

    [Fact]
    public async Task ConcurrentClaims_ExactlyOneWins()
    {
        var store = new InMemoryIdempotencyStore(TimeSpan.FromHours(1));
        var id = Guid.NewGuid();

        var results = await Task.WhenAll(Enumerable.Range(0, 32).Select(_ => Task.Run(() => store.TryClaimAsync(id, Ct).AsTask(), Ct)));

        Assert.Single(results, r => r == IdempotencyClaimResult.Claimed);
    }

    [Fact]
    public void InvalidSettings_Throw()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new InMemoryIdempotencyStore(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InMemoryIdempotencyStore(TimeSpan.FromHours(1), TimeSpan.Zero));
    }
}
