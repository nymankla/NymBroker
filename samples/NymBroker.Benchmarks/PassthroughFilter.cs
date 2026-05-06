using NymBroker.Core.Filter;
using NymBroker.Core.Message;

namespace NymBroker.Benchmarks;

public sealed class PassthroughFilter : IMessageFilter
{
    public IMessageContext? Filter(IMessageContext context) => context;
}
