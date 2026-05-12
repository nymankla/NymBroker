using NymBroker.Core.Consume;
using NymBroker.Core.Message;
using NymBroker.RoutingSample.Messages;

namespace NymBroker.RoutingSample.Subscribers;

public sealed class FraudCheckSubscriber : ISubscribe<OrderMessage>
{
    private const decimal Threshold = 800m;

    public Task ReceiveAsync(OrderMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        if (msg.Amount > Threshold)
            Console.WriteLine($"  [Fraud   ] {msg.OrderId,-8} {msg.Customer,-8} £{msg.Amount,9:N2}  *** flagged: exceeds £{Threshold:N0} ***");
        return Task.CompletedTask;
    }
}
