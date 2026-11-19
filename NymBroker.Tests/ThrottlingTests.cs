using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using NymBroker.Core;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Factory;
using NymBroker.Core.Message;
using NymBroker.Core.Resilience;
using NymBroker.Core.Serialize;
using NymBroker.Tests.CodeHealth;

namespace NymBroker.Tests;

/// <summary>Consumer rate and concurrency limits and <c>LimitSendsTo</c> in the broker (#136).</summary>
public sealed partial class ThrottlingTests
{
    private const string Native = "Native";
    private const string Plain = "Plain";
    private const string Out = "Out";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [MessageName("tests.throttling.job")]
    public sealed class Job
    {
        public int Id { get; set; }
    }

    /// <summary>Counts its calls; <see cref="Gate"/> holds every call until it is released. Tests in one class run one at a time.</summary>
    public sealed class GatedConsumer : IConsume<Job>
    {
        public static TaskCompletionSource Gate = Released();
        public static int Calls;
        public static int Running;
        public static int MaxRunning;

        public static TaskCompletionSource Released()
        {
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            gate.SetResult();
            return gate;
        }

        public static void Reset(bool hold)
        {
            Gate = hold ? new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) : Released();
            Calls = Running = MaxRunning = 0;
        }

        public async Task ConsumeAsync(Job message, IMessageContext context, CancellationToken ct = default)
        {
            Interlocked.Increment(ref Calls);
            var running = Interlocked.Increment(ref Running);
            InterlockedMax(ref MaxRunning, running);
            try { await Gate.Task.WaitAsync(ct); }
            finally { Interlocked.Decrement(ref Running); }
        }

        private static void InterlockedMax(ref int target, int value)
        {
            for (var current = target; value > current; current = target)
                if (Interlocked.CompareExchange(ref target, value, current) == current) return;
        }
    }

    private sealed class FakeEndPoint(bool usesNativeDeadLetter) : IEndPointEventDriven
    {
        public ConcurrentQueue<byte[]> Posted { get; } = new();
        public TaskCompletionSource? Hold { get; set; }
        public int Running;
        public int MaxRunning;
        public bool UsesNativeDeadLetter => usesNativeDeadLetter;

        public async Task PostAsync(byte[] message, CancellationToken ct = default)
        {
            var running = Interlocked.Increment(ref Running);
            if (running > MaxRunning) MaxRunning = running;
            try
            {
                if (Hold is { } hold) await hold.Task.WaitAsync(ct);
                Posted.Enqueue(message);
            }
            finally { Interlocked.Decrement(ref Running); }
        }

        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
        public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct) => Task.CompletedTask;
        public Task StopListeningAsync() => Task.CompletedTask;
    }

    private sealed record Harness(INymBroker Broker, FakeEndPoint OutEndPoint)
    {
        public Task<ProcessResult> ProcessAsync(string source, CancellationToken? ct = null)
            => Broker.ProcessAsync(MessageSerializerJson.SerializeToBytes(new MessageContext<Job> { Message = new Job { Id = 1 } }), source, ct ?? Ct);
    }

    private static Harness Build(Action<NymBrokerBuilder> configure)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var outEndPoint = new FakeEndPoint(usesNativeDeadLetter: false);
        var builder = services.AddNymBroker();
        builder.Services.AddKeyedSingleton<IEndPoint>(Native, new FakeEndPoint(usesNativeDeadLetter: true));
        builder.RegisterEndpoint(Native);
        builder.Services.AddKeyedSingleton<IEndPoint>(Plain, new FakeEndPoint(usesNativeDeadLetter: false));
        builder.RegisterEndpoint(Plain);
        builder.Services.AddKeyedSingleton<IEndPoint>(Out, outEndPoint);
        builder.RegisterEndpoint(Out);
        configure(builder);
        builder.Build();
        return new Harness(services.BuildServiceProvider().GetRequiredService<INymBroker>(), outEndPoint);
    }

    private static RateLimitOptions OnePerSecond(TimeProvider time) => new() { PermitsPerPeriod = 1, TimeProvider = time };

    /// <summary>Lets work that is not waiting on the fake clock run, then checks it is still waiting.</summary>
    private static async Task AssertStillWaitingAsync(Task task)
    {
        await Task.Delay(100, Ct);
        Assert.False(task.IsCompleted);
    }

    // --- Consumer limits ---

    [Fact]
    public async Task ConsumerRateLimit_AMessageOverTheLimitWaits_ThenIsConsumed()
    {
        GatedConsumer.Reset(hold: false);
        var time = new FakeTimeProvider();
        var h = Build(b => b.AddConsumer<GatedConsumer>(o => o.RateLimit = OnePerSecond(time)));
        Assert.Equal(ProcessResult.Completed, await h.ProcessAsync(Native));

        var second = h.ProcessAsync(Native);
        await AssertStillWaitingAsync(second);
        Assert.Equal(1, GatedConsumer.Calls);

        time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(ProcessResult.Completed, await second.WaitAsync(TimeSpan.FromSeconds(10), Ct));
        Assert.Equal(2, GatedConsumer.Calls);
    }

    [Fact]
    public async Task ConsumerConcurrencyLimit_HoldsAcrossEndpoints()
    {
        GatedConsumer.Reset(hold: true);
        var h = Build(b => b.AddConsumer<GatedConsumer>(o => o.ConcurrencyLimit = 1));

        var fromNative = h.ProcessAsync(Native);
        var fromPlain = h.ProcessAsync(Plain);
        await AssertStillWaitingAsync(fromPlain);
        Assert.Equal(1, GatedConsumer.Calls);

        GatedConsumer.Gate.SetResult();
        await Task.WhenAll(fromNative, fromPlain).WaitAsync(TimeSpan.FromSeconds(10), Ct);
        Assert.Equal(2, GatedConsumer.Calls);
        Assert.Equal(1, GatedConsumer.MaxRunning);
    }

    [Fact]
    public async Task ConsumerTimeout_DoesNotCountTheWait()
    {
        GatedConsumer.Reset(hold: false);
        var time = new FakeTimeProvider();
        var h = Build(b => b.AddConsumer<GatedConsumer>(o =>
        {
            o.RateLimit = OnePerSecond(time);
            o.Timeout = TimeSpan.FromMilliseconds(200);
        }));
        await h.ProcessAsync(Native);

        var second = h.ProcessAsync(Native);
        await Task.Delay(400, Ct);   // twice the timeout, waiting for the rate
        Assert.False(second.IsCompleted);

        time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(ProcessResult.Completed, await second.WaitAsync(TimeSpan.FromSeconds(10), Ct));
    }

    [Fact]
    public async Task ConsumerLimit_AStopWhileWaiting_IsACleanExit()
    {
        GatedConsumer.Reset(hold: false);
        var h = Build(b => b.AddConsumer<GatedConsumer>(o => o.RateLimit = OnePerSecond(new FakeTimeProvider())));
        await h.ProcessAsync(Native);
        using var stop = new CancellationTokenSource();

        var waiting = h.ProcessAsync(Native, stop.Token);
        await stop.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        Assert.Equal(1, GatedConsumer.Calls);
    }

    [Fact]
    public async Task ConsumerLimit_AWaitIsNotCountedByTheBreaker()
    {
        GatedConsumer.Reset(hold: false);
        var h = Build(b => b.AddConsumer<GatedConsumer>(o =>
        {
            o.RateLimit = OnePerSecond(new FakeTimeProvider());
            o.CircuitBreaker = new CircuitBreakerOptions { MinimumThroughput = 1, FailureRatio = 1 };
        }));
        await h.ProcessAsync(Native);
        using var stop = new CancellationTokenSource();

        var waiting = h.ProcessAsync(Native, stop.Token);
        await stop.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);

        Assert.Equal(CircuitState.Closed, Assert.Single(h.Broker.Describe().Consumers).CircuitState);
    }

    // --- Send limits ---

    [Fact]
    public async Task SendRateLimit_APostOverTheLimitWaits()
    {
        var time = new FakeTimeProvider();
        var h = Build(b => b.LimitSendsTo(Out, o => o.RateLimit = OnePerSecond(time)));
        await h.Broker.PostAsync(Out, new Job(), Ct);

        var second = h.Broker.PostAsync(Out, new Job(), Ct);
        await AssertStillWaitingAsync(second);
        Assert.Single(h.OutEndPoint.Posted);

        time.Advance(TimeSpan.FromSeconds(1));
        await second.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        Assert.Equal(2, h.OutEndPoint.Posted.Count);
    }

    [Fact]
    public async Task SendRateLimit_ABatchTakesOnePermitPerMessage()
    {
        var time = new FakeTimeProvider();
        var h = Build(b => b.LimitSendsTo(Out, o => o.RateLimit = OnePerSecond(time)));

        var batch = h.Broker.PostBatchAsync(Out, new[] { new Job(), new Job(), new Job() }, Ct);
        time.Advance(TimeSpan.FromSeconds(1));
        await AssertStillWaitingAsync(batch);

        time.Advance(TimeSpan.FromSeconds(1));
        await batch.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        Assert.Equal(3, h.OutEndPoint.Posted.Count);
    }

    [Fact]
    public async Task SendConcurrencyLimit_LetsOneSendRunAtATime()
    {
        var h = Build(b => b.LimitSendsTo(Out, o => o.ConcurrencyLimit = 1));
        var hold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        h.OutEndPoint.Hold = hold;

        var sends = Enumerable.Range(0, 3).Select(_ => h.Broker.PostAsync(Out, new Job(), Ct)).ToList();
        await Task.Delay(100, Ct);
        Assert.Equal(1, h.OutEndPoint.Running);

        hold.SetResult();
        await Task.WhenAll(sends).WaitAsync(TimeSpan.FromSeconds(10), Ct);
        Assert.Equal(1, h.OutEndPoint.MaxRunning);
        Assert.Equal(3, h.OutEndPoint.Posted.Count);
    }

    [Fact]
    public async Task SendLimit_ARouteForwardWaits_HoldingItsSourceMessage()
    {
        var time = new FakeTimeProvider();
        var h = Build(b =>
        {
            b.AddRoute<Job>(r => r.To(Out).WhenFrom(Native));
            b.LimitSendsTo(Out, o => o.RateLimit = OnePerSecond(time));
        });
        Assert.Equal(ProcessResult.Completed, await h.ProcessAsync(Native));

        var second = h.ProcessAsync(Native);
        await AssertStillWaitingAsync(second);

        time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(ProcessResult.Completed, await second.WaitAsync(TimeSpan.FromSeconds(10), Ct));
        Assert.Equal(2, h.OutEndPoint.Posted.Count);
    }

    [Fact]
    public async Task SendLimit_APostWaitingForItsLimit_EndsWithItsToken()
    {
        var h = Build(b => b.LimitSendsTo(Out, o => o.RateLimit = OnePerSecond(new FakeTimeProvider())));
        await h.Broker.PostAsync(Out, new Job(), Ct);
        using var cts = new CancellationTokenSource();

        var waiting = h.Broker.PostAsync(Out, new Job(), cts.Token);
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        Assert.Single(h.OutEndPoint.Posted);
    }

    // --- Registration and description ---

    [Fact]
    public void Describe_ShowsTheLimits()
    {
        var description = Build(b =>
        {
            b.AddConsumer<GatedConsumer>(o => { o.RateLimit = new RateLimitOptions { PermitsPerPeriod = 20 }; o.ConcurrencyLimit = 2; });
            b.LimitSendsTo(Out, o => { o.RateLimit = new RateLimitOptions { PermitsPerPeriod = 50, Burst = 5 }; o.ConcurrencyLimit = 4; });
        }).Broker.Describe();

        var consumer = Assert.Single(description.Consumers);
        Assert.Equal("20/1s, burst 20", consumer.RateLimit);
        Assert.Equal(2, consumer.ConcurrencyLimit);
        var endpoint = description.Endpoints.Single(e => e.Name == Out);
        Assert.Equal("50/1s, burst 5", endpoint.SendRateLimit);
        Assert.Equal(4, endpoint.SendConcurrencyLimit);
        Assert.Null(description.Endpoints.Single(e => e.Name == Plain).SendRateLimit);
    }

    [Fact]
    public void LimitSendsTo_AnUnknownEndpoint_FailsTheBuild()
    {
        var builder = new ServiceCollection().AddNymBroker().LimitSendsTo("Nowhere", o => o.ConcurrencyLimit = 1);

        Assert.Throws<InvalidOperationException>(builder.Build);
    }

    [Fact]
    public void LimitSendsTo_TwiceForOneEndpoint_Throws()
    {
        var builder = new ServiceCollection().AddNymBroker().LimitSendsTo(Out, o => o.ConcurrencyLimit = 1);

        Assert.Throws<InvalidOperationException>(() => builder.LimitSendsTo("out", o => o.ConcurrencyLimit = 2));
    }

    [Fact]
    public void LimitSendsTo_InvalidOrEmptyOptions_Throw()
    {
        var builder = new ServiceCollection().AddNymBroker();

        Assert.ThrowsAny<ArgumentException>(() => builder.LimitSendsTo(Out, _ => { }));
        Assert.ThrowsAny<ArgumentException>(() => builder.LimitSendsTo(Out, o => o.ConcurrencyLimit = 0));
        Assert.ThrowsAny<ArgumentException>(() => builder.LimitSendsTo(Out, o => o.RateLimit = new RateLimitOptions { PermitsPerPeriod = 0 }));
    }

    [Fact]
    public void AddConsumer_InvalidLimits_Throw()
    {
        var builder = new ServiceCollection().AddNymBroker();

        Assert.ThrowsAny<ArgumentException>(() => builder.AddConsumer<GatedConsumer>(o => o.ConcurrencyLimit = 0));
        Assert.ThrowsAny<ArgumentException>(() => builder.AddConsumer<GatedConsumer>(o => o.RateLimit = new RateLimitOptions { Burst = 0 }));
    }

    [Fact]
    public void AddConsumer_SameConsumerWithOtherLimits_Throws()
    {
        var builder = new ServiceCollection().AddNymBroker()
            .AddConsumer<GatedConsumer>(o => o.RateLimit = new RateLimitOptions { PermitsPerPeriod = 5 });

        builder.AddConsumer<GatedConsumer>(o => o.RateLimit = new RateLimitOptions { PermitsPerPeriod = 5 });   // same values: a no-op
        Assert.Throws<InvalidOperationException>(() => builder.AddConsumer<GatedConsumer>(o => o.RateLimit = new RateLimitOptions { PermitsPerPeriod = 6 }));
        Assert.Throws<InvalidOperationException>(() => builder.AddConsumer<GatedConsumer>(o =>
        {
            o.RateLimit = new RateLimitOptions { PermitsPerPeriod = 5 };
            o.ConcurrencyLimit = 1;
        }));
    }

    // --- Guard: a write that bypasses the helpers would bypass the send limit ---

    [GeneratedRegex(@"\.(PostAsync|PostBatchAsync|PostDeferredAsync)\(")]
    private static partial Regex EndpointWrite();

    [Fact]
    public void OnlyTheThrottlingHelpers_WriteToEndpoints()
    {
        var root = CodeMetrics.FindRepoRoot();
        var offenders = new[] { "Impl", "Outbox" }
            .SelectMany(dir => Directory.EnumerateFiles(Path.Combine(root, "NymBroker.Core", dir), "*.cs"))
            .Where(f => Path.GetFileName(f) != "NymBrokerImpl.Throttling.cs")
            .SelectMany(f => File.ReadLines(f).Select((line, i) => (File: Path.GetFileName(f), Line: i + 1, Text: line)))
            .Where(l => EndpointWrite().IsMatch(l.Text) && !l.Text.TrimStart().StartsWith("//", StringComparison.Ordinal))
            .Select(l => $"{l.File}:{l.Line}: {l.Text.Trim()}")
            .ToList();

        Assert.True(offenders.Count == 0,
            "Write to endpoints through WriteAsync / WriteBatchAsync / WriteDeferredAsync (NymBrokerImpl.Throttling.cs), " +
            "or LimitSendsTo is bypassed:\n" + string.Join("\n", offenders));
    }
}
