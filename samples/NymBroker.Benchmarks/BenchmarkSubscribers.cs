using NymBroker.Core.Consume;
using NymBroker.Core.Message;

namespace NymBroker.Benchmarks;

// Three distinct types so each can be registered as a separate keyed service in the 3-subscriber scenario.

public sealed class BenchmarkSubscriberA(CompletionTracker tracker) : ISubscribe<BenchmarkMessage>
{
    public Task ReceiveAsync(BenchmarkMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        tracker.Signal();
        return Task.CompletedTask;
    }
}

public sealed class BenchmarkSubscriberB(CompletionTracker tracker) : ISubscribe<BenchmarkMessage>
{
    public Task ReceiveAsync(BenchmarkMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        tracker.Signal();
        return Task.CompletedTask;
    }
}

public sealed class BenchmarkSubscriberC(CompletionTracker tracker) : ISubscribe<BenchmarkMessage>
{
    public Task ReceiveAsync(BenchmarkMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        tracker.Signal();
        return Task.CompletedTask;
    }
}
