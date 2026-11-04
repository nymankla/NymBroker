using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.File;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;
using Microsoft.Extensions.Logging.Abstractions;

namespace NymBroker.Tests;

public sealed class DeliveryGuaranteeTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public sealed class Big { public string Text { get; set; } = new('x', 5000); }

    private sealed class NativeEndPoint : IEndPointEventDriven
    {
        public ConcurrentQueue<byte[]> Posted { get; } = new();
        public EndpointMode Mode => EndpointMode.ReadWrite;
        public bool UsesNativeDeadLetter => true;
        public Task PostAsync(byte[] message, CancellationToken ct = default) { Posted.Enqueue(message); return Task.CompletedTask; }
        public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct) => Task.CompletedTask;
        public Task StopListeningAsync() => Task.CompletedTask;
        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
    }

    private sealed class ThrowingEndPoint : IEndPoint
    {
        public Task PostAsync(byte[] message, CancellationToken ct = default) => throw new IOException("DLQ down");
        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
    }

    private sealed class FlakyConsumer : IMessageConsumer, IConsume<Big>
    {
        public static int Calls;
        public Task ConsumeAsync(Big message, IMessageContext context, CancellationToken ct = default)
            => Interlocked.Increment(ref Calls) == 1 ? throw new IOException("first attempt fails") : Task.CompletedTask;
    }

    private static NymBrokerImpl BrokerWithConsumer(out ServiceProvider services)
    {
        var collection = new ServiceCollection();
        collection.AddKeyedScoped<IMessageConsumer, FlakyConsumer>(nameof(FlakyConsumer));
        services = collection.BuildServiceProvider();
        var broker = TestBroker.Create(services);
        broker.RegisterConsumer(typeof(Big), nameof(FlakyConsumer));
        return broker;
    }

    [Fact]
    public async Task SplitMessage_RetryOfTheCompletingPart_IsProcessedOnRedelivery()
    {
        FlakyConsumer.Calls = 0;
        var broker = BrokerWithConsumer(out var services);
        await using var _ = services;
        var endpoint = new NativeEndPoint();
        broker.AddEndpoint("Q", endpoint);

        await broker.PostAsync("Q", new Big(), Ct, splitThresholdBytes: 500, compress: false);
        var parts = endpoint.Posted.ToArray();
        Assert.True(parts.Length > 1);

        ProcessResult last = ProcessResult.Completed;
        foreach (var part in parts) last = await broker.ProcessAsync(part, "Q", Ct);
        Assert.Equal(ProcessOutcome.Retry, last.Outcome);   // the transport redelivers only this part

        await broker.ProcessAsync(parts[^1], "Q", Ct);
        Assert.Equal(2, FlakyConsumer.Calls);               // the consumer gets its second attempt
    }

    [Fact]
    public async Task DeadLetterEndpointDown_MessageIsNotReportedCompleted()
    {
        FlakyConsumer.Calls = 0;
        var broker = BrokerWithConsumer(out var services);
        await using var _ = services;
        broker.AddEndpoint("Mem", new MemoryQueueEndPoint("Mem"));
        broker.AddEndpoint("Dlq", new ThrowingEndPoint());
        broker.SetDeadLetterEndpoint("Dlq");
        using var stream = new MessageSerializerJson().Serialize(new MessageContext<Big> { Message = new Big() });
        var bytes = ((MemoryStream)CopyOf(stream)).ToArray();

        var result = await broker.ProcessAsync(bytes, "Mem", Ct);

        Assert.NotEqual(ProcessOutcome.Completed, result.Outcome);
    }

    private static Stream CopyOf(Stream s) { var m = new MemoryStream(); s.CopyTo(m); return m; }

    [Fact]
    public async Task Memory_NoHandlerRunsAfterStopListeningReturns()
    {
        var endpoint = new MemoryQueueEndPoint("Mem");
        var running = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finished = 0;
        await endpoint.PostAsync("{}"u8.ToArray(), Ct);
        await endpoint.StartListeningAsync(async (_, _) =>
        {
            running.TrySetResult();
            await Task.Delay(300, CancellationToken.None);
            Volatile.Write(ref finished, 1);
            return ProcessResult.Completed;
        }, Ct);
        await running.Task.WaitAsync(Ct);

        await endpoint.StopListeningAsync();

        Assert.Equal(1, Volatile.Read(ref finished));
    }

    [Fact]
    public async Task File_NoHandlerRunsAfterStopListeningReturns()
    {
        var folder = Path.Combine(Path.GetTempPath(), $"nymbroker-delivery-{Guid.NewGuid():N}");
        var endpoint = new FileEndPoint("F", new FileSettings { ReadPath = folder, PostPath = folder, IsAbsolutePath = true, PollInterval = TimeSpan.Zero }, NullLogger<FileEndPoint>.Instance);
        var running = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finished = 0;
        await endpoint.PostAsync("{}"u8.ToArray(), Ct);
        await endpoint.StartListeningAsync(async (_, _) =>
        {
            running.TrySetResult();
            await Task.Delay(300, CancellationToken.None);
            Volatile.Write(ref finished, 1);
            return ProcessResult.Completed;
        }, Ct);
        await running.Task.WaitAsync(Ct);

        await endpoint.StopListeningAsync();

        Assert.Equal(1, Volatile.Read(ref finished));
        await Task.Delay(100, CancellationToken.None);
        Directory.Delete(folder, recursive: true);
    }

    [Fact]
    public async Task Memory_CanListenAgainAfterStop()
    {
        var endpoint = new MemoryQueueEndPoint("Mem");
        await endpoint.StartListeningAsync((_, _) => Task.FromResult(ProcessResult.Completed), Ct);
        await endpoint.StopListeningAsync();

        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await endpoint.StartListeningAsync((_, _) => { received.TrySetResult(); return Task.FromResult(ProcessResult.Completed); }, Ct);
        await endpoint.PostAsync("{}"u8.ToArray(), Ct);

        await received.Task.WaitAsync(TimeSpan.FromSeconds(2), Ct);
    }
}
