using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using NymBroker.Core;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Factory;
using NymBroker.Core.Message;
using NymBroker.Core.Resilience;
using NymBroker.Core.Serialize;

namespace NymBroker.Tests;

/// <summary>A consumer's <see cref="ConsumerOptions.Timeout"/> and <see cref="ConsumerOptions.CircuitBreaker"/> in the broker (#134).</summary>
public sealed class ConsumerResilienceTests
{
    private const string Native = "Native";
    private const string Plain = "Plain";
    private const string DeadLetters = "DLQ";
    private static readonly TimeSpan ShortTimeout = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan Break = TimeSpan.FromSeconds(30);

    [MessageName("tests.resilience.job")]
    public sealed class Job
    {
        public int Id { get; set; }
    }

    /// <summary>Ignores its token and never finishes until <see cref="Release"/> — a hung downstream call.</summary>
    public sealed class HangingConsumer : IConsume<Job>
    {
        public static readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task ConsumeAsync(Job message, IMessageContext context, CancellationToken ct = default) => Release.Task;
    }

    /// <summary>Waits on its token, so the timeout or a stop ends it.</summary>
    public sealed class CooperativeSlowConsumer : IConsume<Job>
    {
        public Task ConsumeAsync(Job message, IMessageContext context, CancellationToken ct = default) => Task.Delay(Timeout.Infinite, ct);
    }

    public sealed class QuickConsumer : IConsume<Job>
    {
        public Task ConsumeAsync(Job message, IMessageContext context, CancellationToken ct = default) => Task.CompletedTask;
    }

    /// <summary>Fails while <see cref="Failing"/> is set; counts its calls. Tests in one class run one at a time.</summary>
    public sealed class FlakyConsumer : IConsume<Job>
    {
        public static volatile bool Failing = true;
        public static int Calls;

        public Task ConsumeAsync(Job message, IMessageContext context, CancellationToken ct = default)
        {
            Interlocked.Increment(ref Calls);
            return Failing ? throw new IOException("downstream down") : Task.CompletedTask;
        }
    }

    private sealed class FakeEndPoint(bool usesNativeDeadLetter) : IEndPointEventDriven
    {
        public ConcurrentQueue<byte[]> Posted { get; } = new();
        public bool UsesNativeDeadLetter => usesNativeDeadLetter;

        public Task PostAsync(byte[] message, CancellationToken ct = default)
        {
            Posted.Enqueue(message);
            return Task.CompletedTask;
        }

        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
        public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct) => Task.CompletedTask;
        public Task StopListeningAsync() => Task.CompletedTask;
    }

    private sealed record Harness(INymBroker Broker, MemoryQueueEndPoint DeadLetterQueue, MessageSerializerJson Serializer)
    {
        public Task<ProcessResult> ProcessAsync(string source, CancellationToken? ct = null)
            => Broker.ProcessAsync(MessageSerializerJson.SerializeToBytes(new MessageContext<Job> { Message = new Job { Id = 1 } }), source,
                ct ?? TestContext.Current.CancellationToken);

        public async Task<List<string>> DrainDeadLettersAsync()
        {
            var items = new List<string>();
            await foreach (var item in DeadLetterQueue.ReadAsync(TestContext.Current.CancellationToken)) items.Add(item);
            return items;
        }
    }

    private static Harness Build(Action<NymBrokerBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var builder = services.AddNymBroker()
            .AddMemoryEndPoint(DeadLetters)
            .WithDeadLetterEndpoint(DeadLetters);
        builder.Services.AddKeyedSingleton<IEndPoint>(Native, new FakeEndPoint(usesNativeDeadLetter: true));
        builder.RegisterEndpoint(Native);
        builder.Services.AddKeyedSingleton<IEndPoint>(Plain, new FakeEndPoint(usesNativeDeadLetter: false));
        builder.RegisterEndpoint(Plain);
        configure(builder);
        builder.Build();

        var sp = services.BuildServiceProvider();
        return new Harness(
            sp.GetRequiredService<INymBroker>(),
            (MemoryQueueEndPoint)sp.GetRequiredKeyedService<IEndPoint>(DeadLetters),
            sp.GetRequiredService<MessageSerializerJson>());
    }

    // --- Timeout ---

    [Fact]
    public async Task Timeout_AConsumerThatIgnoresItsToken_DoesNotHoldTheListener()
    {
        var h = Build(b => b.AddConsumer<HangingConsumer>(o => o.Timeout = ShortTimeout));

        var result = await h.ProcessAsync(Native).WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.Equal(ProcessOutcome.Retry, result.Outcome);
        Assert.IsType<TimeoutException>(result.Exception, exactMatch: false);
        HangingConsumer.Release.TrySetResult();   // the abandoned call ends; its outcome is only logged
    }

    [Fact]
    public async Task Timeout_RetryOff_Native_DeadLettersWithConsumerTimedOut()
    {
        var h = Build(b => b.AddConsumer<CooperativeSlowConsumer>(o => { o.Timeout = ShortTimeout; o.RetryOnFailure = false; }));

        var result = await h.ProcessAsync(Native);

        Assert.Equal(ProcessOutcome.DeadLetter, result.Outcome);
        Assert.Equal(DeadLetterReasons.ConsumerTimedOut, result.Reason);
    }

    [Fact]
    public async Task Timeout_Plain_PostsToTheDeadLetterEndpointWithConsumerTimedOut()
    {
        var h = Build(b => b.AddConsumer<CooperativeSlowConsumer>(o => o.Timeout = ShortTimeout));

        var result = await h.ProcessAsync(Plain);

        Assert.Equal(ProcessResult.Completed, result);
        var item = Assert.Single(await h.DrainDeadLettersAsync());
        var ctx = (RawMessageContext)new MessageSerializerJson().Deserialize(item);
        Assert.Equal(DeadLetterReasons.ConsumerTimedOut, ctx.DeadLetter!.Reason);
    }

    [Fact]
    public async Task Timeout_AConsumerThatFinishesInTime_Completes()
    {
        var h = Build(b => b.AddConsumer<QuickConsumer>(o => o.Timeout = TimeSpan.FromSeconds(30)));

        Assert.Equal(ProcessResult.Completed, await h.ProcessAsync(Native));
    }

    [Fact]
    public async Task Timeout_TheStopTokenIsStillACleanExit()
    {
        var h = Build(b => b.AddConsumer<CooperativeSlowConsumer>(o => o.Timeout = TimeSpan.FromMinutes(5)));
        using var stop = new CancellationTokenSource();

        var processing = h.ProcessAsync(Native, stop.Token);
        await stop.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => processing);
    }

    // --- Circuit breaker ---

    private static Action<NymBrokerBuilder> WithFlakyBreaker(FakeTimeProvider time) => b => b.AddConsumer<FlakyConsumer>(o => o.CircuitBreaker = new CircuitBreakerOptions
    {
        MinimumThroughput = 2,
        FailureRatio = 1,
        BreakDuration = Break,
        TimeProvider = time
    });

    [Fact]
    public async Task Breaker_WhileOpen_HoldsTheMessage_ThenTriesItAfterTheBreak()
    {
        FlakyConsumer.Failing = true;
        FlakyConsumer.Calls = 0;
        var time = new FakeTimeProvider();
        var h = Build(WithFlakyBreaker(time));
        Assert.Equal(ProcessOutcome.Retry, (await h.ProcessAsync(Native)).Outcome);
        Assert.Equal(ProcessOutcome.Retry, (await h.ProcessAsync(Native)).Outcome);
        Assert.Equal(CircuitState.Open, Assert.Single(h.Broker.Describe().Consumers).CircuitState);

        var held = h.ProcessAsync(Native);
        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.False(held.IsCompleted);
        Assert.Equal(2, FlakyConsumer.Calls);   // not called while open

        FlakyConsumer.Failing = false;
        time.Advance(Break);

        Assert.Equal(ProcessResult.Completed, await held.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.Equal(3, FlakyConsumer.Calls);
        Assert.Equal(CircuitState.Closed, Assert.Single(h.Broker.Describe().Consumers).CircuitState);
    }

    [Fact]
    public async Task Breaker_AStopWhileHeld_IsACleanExit()
    {
        FlakyConsumer.Failing = true;
        FlakyConsumer.Calls = 0;
        var h = Build(WithFlakyBreaker(new FakeTimeProvider()));
        await h.ProcessAsync(Native);
        await h.ProcessAsync(Native);
        using var stop = new CancellationTokenSource();

        var held = h.ProcessAsync(Native, stop.Token);
        await stop.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => held);
        Assert.Equal(2, FlakyConsumer.Calls);
    }

    [Fact]
    public async Task Breaker_ATimeoutCountsAsAFailure()
    {
        var time = new FakeTimeProvider();
        var h = Build(b => b.AddConsumer<CooperativeSlowConsumer>(o =>
        {
            o.Timeout = ShortTimeout;
            o.CircuitBreaker = new CircuitBreakerOptions { MinimumThroughput = 1, FailureRatio = 1, TimeProvider = time };
        }));

        await h.ProcessAsync(Native);

        var consumer = Assert.Single(h.Broker.Describe().Consumers);
        Assert.Equal(CircuitState.Open, consumer.CircuitState);
        Assert.Equal(ShortTimeout, consumer.Timeout);
    }

    // --- Registration ---

    [Fact]
    public void Describe_WithoutResilience_ShowsNone()
    {
        var consumer = Assert.Single(Build(b => b.AddConsumer<QuickConsumer>()).Broker.Describe().Consumers);

        Assert.Null(consumer.Timeout);
        Assert.Null(consumer.CircuitState);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(2 * 24 * 60 * 60 * 1000.0)]
    public void AddConsumer_InvalidTimeout_Throws(double milliseconds)
    {
        var builder = new ServiceCollection().AddNymBroker();

        Assert.ThrowsAny<ArgumentException>(() => builder.AddConsumer<QuickConsumer>(o => o.Timeout = TimeSpan.FromMilliseconds(milliseconds)));
    }

    [Fact]
    public void AddConsumer_InvalidBreaker_Throws()
    {
        var builder = new ServiceCollection().AddNymBroker();

        Assert.ThrowsAny<ArgumentException>(() => builder.AddConsumer<QuickConsumer>(o => o.CircuitBreaker = new CircuitBreakerOptions { MinimumThroughput = 0 }));
    }

    [Fact]
    public void AddConsumer_SameConsumerWithOtherResilienceOptions_Throws()
    {
        var builder = new ServiceCollection().AddNymBroker()
            .AddConsumer<QuickConsumer>(o => o.Timeout = TimeSpan.FromSeconds(5));

        builder.AddConsumer<QuickConsumer>(o => o.Timeout = TimeSpan.FromSeconds(5));   // same options: a no-op
        Assert.Throws<InvalidOperationException>(() => builder.AddConsumer<QuickConsumer>(o => o.Timeout = TimeSpan.FromSeconds(6)));
        Assert.Throws<InvalidOperationException>(() => builder.AddConsumer<QuickConsumer>(o =>
        {
            o.Timeout = TimeSpan.FromSeconds(5);
            o.CircuitBreaker = new CircuitBreakerOptions();
        }));
    }
}
