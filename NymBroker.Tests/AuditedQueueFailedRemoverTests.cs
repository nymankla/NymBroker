using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Endpoint.Queue;

namespace NymBroker.Tests;

/// <summary>The audit trail <see cref="Core.INymBroker.GetQueueFailedRemover"/> adds around an endpoint's remover.</summary>
public sealed class AuditedQueueFailedRemoverTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly QueueMessageInfo Failed = new(Guid.NewGuid().ToString(), QueueInspectionState.Failed, 5,
        DateTimeOffset.UtcNow, null, DateTimeOffset.UtcNow, "ConsumerFailed: boom", ReadOnlyMemory<byte>.Empty);

    private sealed class FakeRemover(Func<IReadOnlyCollection<QueueMessageInfo>, QueueRemoveResult> remove) : IQueueFailedRemover
    {
        public int Calls;

        public Task<QueueRemoveResult> RemoveFailedAsync(IReadOnlyCollection<QueueMessageInfo> messages, CancellationToken ct = default)
        {
            Calls++;
            return Task.FromResult(remove(messages));
        }
    }

    [Fact]
    public async Task Remove_LogsAWarningWithTheIds_AndCountsTheRemovedMessages()
    {
        var endpoint = $"orders-{Guid.NewGuid():N}";
        var logger = new CapturingLogger<AuditedQueueFailedRemoverTests>();
        var inner = new FakeRemover(_ => new QueueRemoveResult(["r1", "r2", "r3"], ["s1"]));
        long counted = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == NymBrokerDiagnostics.InstrumentationName && instrument.Name == "nymbroker.messages.removed")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            foreach (var tag in tags)
                if (tag is { Key: "endpoint" } && (string?)tag.Value == endpoint) Interlocked.Add(ref counted, value);
        });
        listener.Start();

        var result = await new AuditedQueueFailedRemover(endpoint, inner, logger).RemoveFailedAsync([Failed], Ct);

        Assert.Equal(["r1", "r2", "r3"], result.Removed);
        Assert.Equal(3, Interlocked.Read(ref counted));
        var entry = Assert.Single(logger.Entries);
        Assert.Equal(LogLevel.Warning, entry.Level);
        Assert.Contains(endpoint, entry.Message);
        Assert.Contains("r1", entry.Message);
        Assert.Contains("s1", entry.Message);
    }

    [Fact]
    public async Task FailedRemoval_LogsAnError_AndRethrows()
    {
        var logger = new CapturingLogger<AuditedQueueFailedRemoverTests>();
        var inner = new FakeRemover(_ => throw new InvalidOperationException("db down"));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            new AuditedQueueFailedRemover("orders", inner, logger).RemoveFailedAsync([Failed], Ct));

        var entry = Assert.Single(logger.Entries);
        Assert.Equal(LogLevel.Error, entry.Level);
        Assert.IsType<InvalidOperationException>(entry.Exception);
    }

    [Fact]
    public async Task InvalidSelection_IsRejected_BeforeTheEndpointIsCalled()
    {
        var inner = new FakeRemover(_ => new QueueRemoveResult([], []));
        var remover = new AuditedQueueFailedRemover("orders", inner, new CapturingLogger<AuditedQueueFailedRemoverTests>());

        await Assert.ThrowsAsync<ArgumentNullException>(() => remover.RemoveFailedAsync(null!, Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => remover.RemoveFailedAsync([], Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => remover.RemoveFailedAsync([Failed with { State = QueueInspectionState.Pending }], Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => remover.RemoveFailedAsync([Failed with { FailedAt = null }], Ct));
        Assert.Equal(0, inner.Calls);
    }
}
