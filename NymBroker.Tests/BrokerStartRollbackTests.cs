using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.File;
using NymBroker.Core.Scheduling;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Impl;
using NymBroker.Core.Outbox;

namespace NymBroker.Tests;

/// <summary>
/// A failed <c>StartAsync</c> leaves nothing running and nothing waiting, and a background loop that is stopped while
/// a store call is being cancelled does not report a crash.
/// </summary>
public sealed class BrokerStartRollbackTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private sealed class Ping { public int Value { get; set; } }

    /// <summary>Shared by the endpoints of one broker: the endpoint started as number <see cref="FailAt"/> throws.</summary>
    private sealed class StartPlan
    {
        public int FailAt { get; set; } = 2;
        public int Started;
    }

    private sealed class PlannedEndPoint(StartPlan plan) : IEndPointEventDriven
    {
        public EndpointMode Mode => EndpointMode.ReadWrite;
        public bool Listening { get; private set; }
        public int StopCalls { get; private set; }

        /// <summary>The result of a message delivered as soon as the endpoint listens, while the broker is still starting.</summary>
        public Task<ProcessResult>? EarlyDelivery { get; private set; }

        public Task PostAsync(byte[] message, CancellationToken ct = default) => Task.CompletedTask;
        public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();

        public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct)
        {
            if (Interlocked.Increment(ref plan.Started) == plan.FailAt)
                throw new IOException("transport unavailable");

            Listening = true;
            EarlyDelivery = handler("{}"u8.ToArray(), CancellationToken.None);
            return Task.CompletedTask;
        }

        public Task StopListeningAsync()
        {
            StopCalls++;
            Listening = false;
            return Task.CompletedTask;
        }
    }

    private static NymBrokerImpl CreateBroker()
        => TestBroker.Create(new ServiceCollection().BuildServiceProvider());

    [Fact]
    public async Task StartAsync_AListenerFailsToStart_StopsTheListenersAlreadyStarted()
    {
        var plan = new StartPlan();
        var endpoints = new[] { new PlannedEndPoint(plan), new PlannedEndPoint(plan) };
        var broker = CreateBroker();
        broker.AddEndpoint("A", endpoints[0]);
        broker.AddEndpoint("B", endpoints[1]);

        await Assert.ThrowsAsync<IOException>(() => broker.StartAsync(Ct));

        var started = Assert.Single(endpoints, e => e.EarlyDelivery is not null);
        Assert.False(started.Listening);
        Assert.Equal(1, started.StopCalls);

        // The message it delivered during the start is handed back, not left waiting for a start that never comes.
        var result = await started.EarlyDelivery!.WaitAsync(Timeout, Ct);
        Assert.Equal(ProcessOutcome.Retry, result.Outcome);
    }

    [Fact]
    public async Task StartAsync_AfterAFailedStart_StartsAgain()
    {
        var plan = new StartPlan();
        var endpoints = new[] { new PlannedEndPoint(plan), new PlannedEndPoint(plan) };
        var broker = CreateBroker();
        broker.AddEndpoint("A", endpoints[0]);
        broker.AddEndpoint("B", endpoints[1]);
        await Assert.ThrowsAsync<IOException>(() => broker.StartAsync(Ct));

        plan.FailAt = -1;
        await broker.StartAsync(Ct);
        try
        {
            Assert.All(endpoints, e => Assert.True(e.Listening));
            // Delivered during the second start: held until it finished, then processed.
            foreach (var endpoint in endpoints)
                Assert.Equal(ProcessOutcome.Completed, (await endpoint.EarlyDelivery!.WaitAsync(Timeout, Ct)).Outcome);
        }
        finally
        {
            await broker.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task StartAsync_ConfigurationIsInvalid_PublishDoesNotWaitForAStart()
    {
        var broker = CreateBroker();
        broker.SetDeadLetterEndpoint("Missing");
        await Assert.ThrowsAsync<InvalidOperationException>(() => broker.StartAsync(Ct));

        // No consumer, route or topic: processed as "nothing to do", as on a broker that was never started.
        await broker.PublishAsync(new Ping { Value = 1 }, Ct).WaitAsync(Timeout, Ct);
    }

    /// <summary>Reports the cancellation of a claim the way some database providers do: as its own exception type.</summary>
    private sealed class CancelledAsProviderErrorStore : IOutboxStore
    {
        public TaskCompletionSource Claiming { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<IReadOnlyList<ClaimedOutboxMessage>> ClaimAsync(Guid claimId, int maxCount, TimeSpan leaseTimeout, CancellationToken ct = default)
        {
            Claiming.TrySetResult();
            try
            {
                await Task.Delay(System.Threading.Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
                throw new InvalidOperationException("Operation cancelled by user.");
            }
            return [];
        }

        public Task AddAsync(DbTransaction transaction, OutboxMessage message, CancellationToken ct = default) => Task.CompletedTask;
        public Task MarkSentAsync(long id, Guid claimId, CancellationToken ct = default) => Task.CompletedTask;
        public Task RetryLaterAsync(long id, Guid claimId, TimeSpan delay, string error, CancellationToken ct = default) => Task.CompletedTask;
        public Task MarkFailedAsync(long id, Guid claimId, string error, CancellationToken ct = default) => Task.CompletedTask;
        public Task ReleaseAsync(long id, Guid claimId, CancellationToken ct = default) => Task.CompletedTask;
    }

    /// <summary>A schedule store whose claim reports cancellation as its own exception type.</summary>
    private sealed class CancelledAsProviderErrorScheduleStore : IScheduleStore
    {
        public TaskCompletionSource Claiming { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask<DateTimeOffset?> GetLastRunAsync(string name, CancellationToken ct = default) => ValueTask.FromResult<DateTimeOffset?>(null);

        public async ValueTask<bool> TryClaimAsync(string name, DateTimeOffset occurrence, CancellationToken ct = default)
        {
            Claiming.TrySetResult();
            try
            {
                await Task.Delay(System.Threading.Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
                throw new InvalidOperationException("Operation cancelled by user.");
            }
            return true;
        }
    }

    [Fact]
    public async Task NamedScheduledAction_StoppedWhileTheStoreReportsCancellationAsAnotherException_LogsNoFailure()
    {
        var store = new CancelledAsProviderErrorScheduleStore();
        var logger = new CapturingLogger<NymBrokerImpl>();
        var broker = TestBroker.Create(new ServiceCollection().BuildServiceProvider(), logger: logger);
        broker.SetScheduleStore(store);
        broker.AddNamedScheduledAction("cleanup", new GridTiming(TimeSpan.FromMilliseconds(20)), new ScheduledActionOptions(), _ => Task.CompletedTask);

        await broker.StartAsync(Ct);
        await store.Claiming.Task.WaitAsync(Timeout, Ct);
        await broker.StopAsync(CancellationToken.None);

        Assert.DoesNotContain(logger.Entries, e => e.Level >= LogLevel.Error);
    }

    [Fact]
    public async Task FileEndPoint_StartFailsBecauseTheFolderIsGone_CanBeStartedAgain()
    {
        var folder = Path.Combine(Path.GetTempPath(), $"nymbroker-start-{Guid.NewGuid():N}");
        var endpoint = new FileEndPoint("F", new FileSettings { ReadPath = folder, PostPath = folder, IsAbsolutePath = true, PollInterval = TimeSpan.Zero },
            NullLogger<FileEndPoint>.Instance);
        Task<ProcessResult> Handle(byte[] raw, CancellationToken ct) => Task.FromResult(ProcessResult.Completed);
        try
        {
            Directory.Delete(folder);
            await Assert.ThrowsAnyAsync<Exception>(() => endpoint.StartListeningAsync(Handle, Ct));

            // Not "already listening": the failed start left no reader loop behind.
            Directory.CreateDirectory(folder);
            await endpoint.StartListeningAsync(Handle, Ct);
        }
        finally
        {
            await endpoint.StopListeningAsync();
            if (Directory.Exists(folder)) Directory.Delete(folder, recursive: true);
        }
    }

    [Fact]
    public async Task OutboxDispatcher_StoppedWhileTheStoreReportsCancellationAsAnotherException_LogsNoFailure()
    {
        var store = new CancelledAsProviderErrorStore();
        var logger = new CapturingLogger<OutboxDispatcher>();
        using var dispatcher = new OutboxDispatcher(store, CreateBroker(), new OutboxOptions(), logger);

        await dispatcher.StartAsync(Ct);
        await store.Claiming.Task.WaitAsync(Timeout, Ct);
        await dispatcher.StopAsync(CancellationToken.None);

        Assert.DoesNotContain(logger.Entries, e => e.Level >= LogLevel.Warning);
    }
}
