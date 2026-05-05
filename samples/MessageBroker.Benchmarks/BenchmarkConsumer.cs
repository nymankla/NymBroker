using MessageBroker.Core.Consume;
using MessageBroker.Core.Message;

namespace MessageBroker.Benchmarks;

public sealed class BenchmarkConsumer(CompletionTracker tracker) : IMessageConsumer, IConsume<BenchmarkMessage>
{
    public string Name => nameof(BenchmarkConsumer);

    public Task ConsumeAsync(BenchmarkMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        tracker.Signal();
        return Task.CompletedTask;
    }
}
