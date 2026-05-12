using NymBroker.Core.Consume;
using NymBroker.Core.Message;
using NymBroker.RoutingSample.Messages;

namespace NymBroker.RoutingSample.Consumers;

public sealed class OrderConsumer : IConsume<OrderMessage>
{
    public Task ConsumeAsync(OrderMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        var queue = ctx.Address?.From ?? "?";
        var tag   = queue == "VIP" ? "[VIP     ]" : "[Standard]";
        Console.WriteLine($"  {tag} {msg.OrderId,-8} {msg.Customer,-8} £{msg.Amount,9:N2}  [{msg.Priority}]");
        return Task.CompletedTask;
    }
}
