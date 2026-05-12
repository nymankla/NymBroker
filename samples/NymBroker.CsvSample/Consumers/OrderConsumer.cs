using NymBroker.Core.Consume;
using NymBroker.Core.Message;
using NymBroker.CsvSample.Messages;
using Microsoft.Extensions.Logging;

namespace NymBroker.CsvSample.Consumers;

public class OrderConsumer(ILogger<OrderConsumer> logger) : IConsume<OrderMessage>
{
    public Task ConsumeAsync(OrderMessage message, IMessageContext context, CancellationToken ct = default)
    {
        logger.LogInformation("Order received: {OrderId}  customer={Customer}  amount={Amount:F2}  priority={Priority}",
            message.OrderId, message.Customer, message.Amount, message.Priority);
        return Task.CompletedTask;
    }
}
