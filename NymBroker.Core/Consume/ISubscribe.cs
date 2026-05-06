using NymBroker.Core.Message;

namespace NymBroker.Core.Consume;

public interface ISubscribe<T> : IMessageSubscriber where T : class
{
    Task ReceiveAsync(T message, IMessageContext context, CancellationToken ct = default);
}
