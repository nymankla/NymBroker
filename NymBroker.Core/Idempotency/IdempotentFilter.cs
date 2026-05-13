using NymBroker.Core.Filter;
using NymBroker.Core.Message;

namespace NymBroker.Core.Idempotency;

public sealed class IdempotentFilter(IIdempotencyStore store) : IMessageFilter
{
    public IMessageContext? Filter(IMessageContext context)
        => store.TryMarkSeen(context.Id) ? context : null;
}
