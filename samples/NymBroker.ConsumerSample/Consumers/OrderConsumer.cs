using Microsoft.Extensions.Logging;
using NymBroker.ConsumerSample.Messages;
using NymBroker.Core.Consume;
using NymBroker.Core.Message;

namespace NymBroker.ConsumerSample.Consumers;

public sealed class OrderConsumer : IConsume<OrderMessage>
{
    private readonly ILogger<OrderConsumer> _logger;

    public OrderConsumer(ILogger<OrderConsumer> logger) => _logger = logger;

    public Task ConsumeAsync(OrderMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        _logger.LogInformation(
            "Received order {Id} from {Customer} — {Amount:C} [{Priority}]",
            msg.OrderId, msg.Customer, msg.Amount, msg.Priority);
        return Task.CompletedTask;
    }
}
