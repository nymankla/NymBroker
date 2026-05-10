using NymBroker.ConsumerSample.Messages;
using NymBroker.Core.Consume;
using NymBroker.Core.Message;
using Microsoft.Extensions.Logging;

namespace NymBroker.ConsumerSample.Consumers;

public sealed class OrderConsumer(ILogger<OrderConsumer> logger) : IConsume<OrderMessage>
{
    public string Name => nameof(OrderConsumer);

    public Task ConsumeAsync(OrderMessage message, IMessageContext context, CancellationToken ct = default)
    {
        logger.LogInformation(
            "Received order {OrderId} | Customer: {Customer} | Amount: {Amount:C} | Priority: {Priority}",
            message.OrderId,
            message.Customer,
            message.Amount,
            message.Priority);

        return Task.CompletedTask;
    }
}
