using NymBroker.Core.Consume;
using NymBroker.Core.Message;
using NymBroker.RabbitSample.Messages;

namespace NymBroker.RabbitSample.Consumers;

public sealed class OrderConsumer : IConsume<OrderMessage>
{
    public Task ConsumeAsync(OrderMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        Console.WriteLine($"  [{msg.Priority,-6}] {msg.OrderId,-8} {msg.Customer,-8} £{msg.Amount,9:N2}");
        return Task.CompletedTask;
    }
}
