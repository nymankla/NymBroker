using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Endpoint.Queue;

namespace NymBroker.Tests;

/// <summary>The audit trail <see cref="Core.INymBroker.GetQueueReplayer"/> adds around an endpoint's replayer.</summary>
public sealed class AuditedQueueReplayerTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly QueueMessageInfo Failed = new(Guid.NewGuid().ToString(), QueueInspectionState.Failed, 5,
        DateTimeOffset.UtcNow, null, DateTimeOffset.UtcNow, "ConsumerFailed: boom", ReadOnlyMemory<byte>.Empty);

    private sealed class FakeReplayer(Func<IReadOnlyCollection<QueueMessageInfo>, QueueReplayResult> replay) : IQueueReplayer
    {
        public int Calls;

        public Task<QueueReplayResult> ReplayFailedAsync(IReadOnlyCollection<QueueMessageInfo> messages, CancellationToken ct = default)
        {
            Calls++;
            return Task.FromResult(replay(messages));
        }
    }

    [Fact]
    public async Task Replay_LogsAWarningWithTheIds_AndCountsTheReplayedMessages()
    {
        var endpoint = $"orders-{Guid.NewGuid():N}";
        var logger = new CapturingLogger<AuditedQueueReplayerTests>();
        var inner = new FakeReplayer(_ => new QueueReplayResult(["r1", "r2"], ["s1"]));
        long counted = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == NymBrokerDiagnostics.InstrumentationName && instrument.Name == "nymbroker.messages.replayed")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            foreach (var tag in tags)
                if (tag is { Key: "endpoint" } && (string?)tag.Value == endpoint) Interlocked.Add(ref counted, value);
        });
        listener.Start();

        var result = await new AuditedQueueReplayer(endpoint, inner, logger).ReplayFailedAsync([Failed], Ct);

        Assert.Equal(["r1", "r2"], result.Replayed);
        Assert.Equal(2, Interlocked.Read(ref counted));
        var entry = Assert.Single(logger.Entries);
        Assert.Equal(LogLevel.Warning, entry.Level);
        Assert.Contains(endpoint, entry.Message);
        Assert.Contains("r1", entry.Message);
        Assert.Contains("s1", entry.Message);
    }

    [Fact]
    public async Task FailedReplay_LogsAnError_AndRethrows()
    {
        var logger = new CapturingLogger<AuditedQueueReplayerTests>();
        var inner = new FakeReplayer(_ => throw new InvalidOperationException("db down"));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            new AuditedQueueReplayer("orders", inner, logger).ReplayFailedAsync([Failed], Ct));

        var entry = Assert.Single(logger.Entries);
        Assert.Equal(LogLevel.Error, entry.Level);
        Assert.IsType<InvalidOperationException>(entry.Exception);
    }

    [Fact]
    public async Task InvalidSelection_IsRejected_BeforeTheEndpointIsCalled()
    {
        var inner = new FakeReplayer(_ => new QueueReplayResult([], []));
        var replayer = new AuditedQueueReplayer("orders", inner, new CapturingLogger<AuditedQueueReplayerTests>());

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => replayer.ReplayFailedAsync([], Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => replayer.ReplayFailedAsync([Failed with { State = QueueInspectionState.Pending }], Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => replayer.ReplayFailedAsync([Failed with { FailedAt = null }], Ct));
        Assert.Equal(0, inner.Calls);
    }
}
