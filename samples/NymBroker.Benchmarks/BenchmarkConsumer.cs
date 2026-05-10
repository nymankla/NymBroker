using NymBroker.Core.Consume;
using NymBroker.Core.Message;

namespace NymBroker.Benchmarks;

public sealed class BenchmarkConsumer(CompletionTracker tracker) : IMessageConsumer, IConsume<BenchmarkMessage>
{
    public Task ConsumeAsync(BenchmarkMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        tracker.Signal();
        return Task.CompletedTask;
    }
}
