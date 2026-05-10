using NymBroker.Core.Consume;
using NymBroker.Core.Message;
using NymBroker.SqlSample.Messages;
using Microsoft.Extensions.Logging;

namespace NymBroker.SqlSample.Consumers;

public sealed class OrderConsumer : IConsume<OrderMessage>
{
    private readonly ILogger<OrderConsumer> _logger;

    public OrderConsumer(ILogger<OrderConsumer> logger) => _logger = logger;

    public Task ConsumeAsync(OrderMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        _logger.LogInformation(
            "[SQL] Order {Id} from {Customer} — {Amount:C}  [{Priority}]",
            msg.OrderId, msg.Customer, msg.Amount, msg.Priority);
        return Task.CompletedTask;
    }
}
