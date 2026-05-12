using NymBroker.Core.Consume;
using NymBroker.Core.Message;
using NymBroker.RoutingSample.Messages;

namespace NymBroker.RoutingSample.Subscribers;

public sealed class AuditSubscriber : ISubscribe<OrderMessage>
{
    public Task ReceiveAsync(OrderMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        Console.WriteLine($"  [Audit   ] {msg.OrderId,-8} {msg.Customer,-8} £{msg.Amount,9:N2}  recorded");
        return Task.CompletedTask;
    }
}
