using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Core.Factory.Configuration;
using NymBroker.Endpoint.MySql;
using NymBroker.Endpoint.Postgres;
using NymBroker.Endpoint.Sqlite;
using NymBroker.Endpoint.SqlServer;

namespace NymBroker.Tests;

/// <summary><see cref="ILeasedQueueSettings.MaxConcurrentCalls"/>: handling the messages of a claimed batch in parallel (#126).</summary>
public sealed class LeasedQueueConcurrencyTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private sealed class Settings : ILeasedQueueSettings
    {
        public int BatchSize { get; init; } = 20;
        public TimeSpan PollInterval => TimeSpan.FromMilliseconds(5);
        public TimeSpan LeaseTimeout => TimeSpan.FromMinutes(1);
        public int MaxRetryCount => 3;
        public bool UseNativeDeadLetter => true;
        public int MaxConcurrentCalls { get; init; } = 1;
    }

    /// <summary>A transport written before the setting existed.</summary>
    private sealed class MinimalSettings : ILeasedQueueSettings
    {
        public int BatchSize => 20;
        public TimeSpan PollInterval => TimeSpan.FromMilliseconds(5);
        public TimeSpan LeaseTimeout => TimeSpan.FromMinutes(1);
        public int MaxRetryCount => 3;
        public bool UseNativeDeadLetter => true;
    }

    /// <summary>Hands out one batch, then nothing; records every outcome written, by the loop or on shutdown.</summary>
    private sealed class OneBatchListener(ILeasedQueueSettings settings, ILogger logger, int count)
        : LeasedQueueListener("Fake", settings, logger)
    {
        private int _claims;

        public ConcurrentQueue<QueueMessageOutcome> Written { get; } = new();

        protected override Task<IReadOnlyList<QueueMessage>> FinalizeAndClaimAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
        {
            Record(outcomes);
            IReadOnlyList<QueueMessage> batch = Interlocked.Increment(ref _claims) == 1
                ? [.. Enumerable.Range(0, count).Select(i => new QueueMessage(i, 1, [(byte)i]))]
                : [];
            return Task.FromResult(batch);
        }

        protected override Task FinalizeAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
        {
            Record(outcomes);
            return Task.CompletedTask;
        }

        private void Record(IReadOnlyList<QueueMessageOutcome> outcomes)
        {
            foreach (var outcome in outcomes) Written.Enqueue(outcome);
        }
    }

    /// <summary>Counts the handlers running at once.</summary>
    private sealed class Concurrency
    {
        private int _running;
        private int _max;

        public int Max => Volatile.Read(ref _max);

        public async Task<ProcessResult> RunAsync(Func<Task> work)
        {
            var now = Interlocked.Increment(ref _running);
            int max;
            while (now > (max = Volatile.Read(ref _max)) && Interlocked.CompareExchange(ref _max, now, max) != max) { }
            try
            {
                await work();
                return ProcessResult.Completed;
            }
            finally
            {
                Interlocked.Decrement(ref _running);
            }
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "Condition was not met in time.");
            await Task.Delay(5, Ct);
        }
    }

    [Fact]
    public async Task HandlesAtMostMaxConcurrentCallsAtOnce_AndWritesEveryOutcome()
    {
        var listener = new OneBatchListener(new Settings { MaxConcurrentCalls = 4 }, new CapturingLogger<OneBatchListener>(), count: 12);
        var concurrency = new Concurrency();

        listener.Start((_, ct) => concurrency.RunAsync(() => Task.Delay(50, ct)), Ct);
        await WaitUntilAsync(() => listener.Written.Count == 12);
        await listener.StopAsync();

        Assert.Equal(4, concurrency.Max);
        Assert.Equal(Enumerable.Range(0, 12).Select(i => (long)i), listener.Written.Select(o => o.Message.QueueId).Order());
        Assert.All(listener.Written, o => Assert.Equal(QueueMessageStatus.Completed, o.Status));
    }

    [Fact]
    public async Task ByDefault_HandlesOneMessageAtATime_InOrder()
    {
        var listener = new OneBatchListener(new MinimalSettings(), new CapturingLogger<OneBatchListener>(), count: 6);
        var concurrency = new Concurrency();
        var order = new ConcurrentQueue<byte>();

        listener.Start((raw, ct) => concurrency.RunAsync(async () =>
        {
            order.Enqueue(raw[0]);
            await Task.Delay(10, ct);
        }), Ct);
        await WaitUntilAsync(() => listener.Written.Count == 6);
        await listener.StopAsync();

        Assert.Equal(1, concurrency.Max);
        Assert.Equal(new byte[] { 0, 1, 2, 3, 4, 5 }, order);
    }

    [Fact]
    public async Task AFailingMessage_DoesNotAffectTheOthers()
    {
        var logger = new CapturingLogger<OneBatchListener>();
        var listener = new OneBatchListener(new Settings { MaxConcurrentCalls = 3 }, logger, count: 6);

        listener.Start((raw, _) => raw[0] switch
        {
            1 => Task.FromResult(ProcessResult.Retry(description: "try later")),
            2 => throw new InvalidOperationException("consumer failed"),
            3 => Task.FromResult(ProcessResult.DeadLetter("Poison", "never works")),
            _ => Task.FromResult(ProcessResult.Completed)
        }, Ct);
        await WaitUntilAsync(() => listener.Written.Count == 6);
        await listener.StopAsync();

        var status = listener.Written.ToDictionary(o => o.Message.QueueId, o => o.Status);
        Assert.Equal(QueueMessageStatus.Pending, status[1]);
        Assert.Equal(QueueMessageStatus.Pending, status[2]);
        Assert.Equal(QueueMessageStatus.Failed, status[3]);
        Assert.All(new long[] { 0, 4, 5 }, id => Assert.Equal(QueueMessageStatus.Completed, status[id]));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error && e.Exception is InvalidOperationException);
    }

    [Fact]
    public async Task Stop_WritesWhatWasHandled_AndStartsNothingMore()
    {
        var listener = new OneBatchListener(new Settings { MaxConcurrentCalls = 2 }, new CapturingLogger<OneBatchListener>(), count: 6);
        var started = new ConcurrentQueue<byte>();
        var blocked = 0;

        // Message 0 finishes at once; 1 and 2 run until the stop; 3 to 5 must never start.
        listener.Start(async (raw, ct) =>
        {
            started.Enqueue(raw[0]);
            if (raw[0] == 0) return ProcessResult.Completed;
            Interlocked.Increment(ref blocked);
            await Task.Delay(System.Threading.Timeout.Infinite, ct);
            return ProcessResult.Completed;
        }, Ct);
        await WaitUntilAsync(() => Volatile.Read(ref blocked) == 2);
        await listener.StopAsync();

        Assert.Equal(new byte[] { 0, 1, 2 }, started.Order());
        var written = Assert.Single(listener.Written);
        Assert.Equal((0L, QueueMessageStatus.Completed), (written.Message.QueueId, written.Status));
    }

    [Fact]
    public void LessThanOne_FailsWhenTheEndpointIsCreated()
    {
        Assert.Throws<ArgumentException>(() => new SqliteEndPoint("Sql",
            new SqliteSettings { ConnectionString = "Data Source=:memory:", MaxConcurrentCalls = 0 }, new CapturingLogger<SqliteEndPoint>()));
    }

    [Fact]
    public async Task MoreThanTheBatchSize_LogsAWarningAtStart()
    {
        var logger = new CapturingLogger<OneBatchListener>();
        var listener = new OneBatchListener(new Settings { BatchSize = 4, MaxConcurrentCalls = 8 }, logger, count: 0);

        listener.Start((_, _) => Task.FromResult(ProcessResult.Completed), Ct);
        await listener.StopAsync();

        var warning = Assert.Single(logger.Entries, e => e.Level == LogLevel.Warning);
        Assert.Contains("MaxConcurrentCalls 8", warning.Message, StringComparison.Ordinal);
    }

    public static TheoryData<ILeasedQueueSettings> EverySqlEndpoint =>
        [new SqliteSettings(), new PostgresSettings(), new SqlServerSettings(), new MySqlSettings()];

    [Theory]
    [MemberData(nameof(EverySqlEndpoint))]
    public void IsOneByDefault(ILeasedQueueSettings settings) => Assert.Equal(1, settings.MaxConcurrentCalls);

    [Fact]
    public void CanComeFromTheConfigurationFile()
    {
        var configuration = new EndPointConfiguration { Config = JsonDocument.Parse("""{ "maxConcurrentCalls": 8 }""").RootElement };

        Assert.Equal(8, configuration.GetSettings<PostgresSettings>().MaxConcurrentCalls);
    }
}
