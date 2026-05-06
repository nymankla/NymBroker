using MessageBroker.Core.Message;

namespace MessageBroker.Core.Consume;

public interface ISubscribe<T> : IMessageSubscriber where T : class
{
    Task ReceiveAsync(T message, IMessageContext context, CancellationToken ct = default);
}
