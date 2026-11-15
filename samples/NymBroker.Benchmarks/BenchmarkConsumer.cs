using NymBroker.Core.Consume;
using NymBroker.Core.Message;

namespace NymBroker.Benchmarks;

public sealed class BenchmarkConsumer(CompletionTracker tracker) : IMessageConsumer, IConsume<BenchmarkMessage>
{
    public Task ConsumeAsync(BenchmarkMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        if (tracker.ConsumerFails) throw new InvalidOperationException("Benchmark: consumer fails on purpose.");
        if (tracker.ConsumerDelay > TimeSpan.Zero) return SignalAfterDelayAsync(ct);

        tracker.Signal();
        return Task.CompletedTask;
    }

    private async Task SignalAfterDelayAsync(CancellationToken ct)
    {
        await Task.Delay(tracker.ConsumerDelay, ct);
        tracker.Signal();
    }
}
