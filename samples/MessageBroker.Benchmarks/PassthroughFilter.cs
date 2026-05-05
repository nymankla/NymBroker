using MessageBroker.Core.Filter;
using MessageBroker.Core.Message;

namespace MessageBroker.Benchmarks;

public sealed class PassthroughFilter : IMessageFilter
{
    public IMessageContext? Filter(IMessageContext context) => context;
}
