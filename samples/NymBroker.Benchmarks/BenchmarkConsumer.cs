using NymBroker.Core.Consume;
using NymBroker.Core.Message;

namespace NymBroker.Benchmarks;

public sealed class BenchmarkConsumer(CompletionTracker tracker) : IMessageConsumer, IConsume<BenchmarkMessage>
{
    public Task ConsumeAsync(BenchmarkMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        if (tracker.ConsumerFails) throw new InvalidOperationException("Benchmark: consumer fails on purpose.");

        tracker.Signal();
        return Task.CompletedTask;
    }
}
