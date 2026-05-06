using NymBroker.Core.Message;

namespace NymBroker.Core.Consume;

public interface IConsume<T> : IMessageConsumer where T : class
{
    Task ConsumeAsync(T message, IMessageContext context, CancellationToken ct = default);
}
