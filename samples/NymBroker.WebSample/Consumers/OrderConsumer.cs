using Microsoft.Extensions.Logging;
using NymBroker.Core.Consume;
using NymBroker.Core.Message;
using NymBroker.WebSample.Messages;

namespace NymBroker.WebSample.Consumers;

public sealed class OrderConsumer : IConsume<OrderMessage>
{
    private readonly ILogger<OrderConsumer> _logger;

    public OrderConsumer(ILogger<OrderConsumer> logger) => _logger = logger;

    public Task ConsumeAsync(OrderMessage msg, IMessageContext ctx, CancellationToken ct = default)
    {
        _logger.LogInformation(
            "[OrderConsumer] Order {Id} from {Customer} — {Amount:C} [{Priority}]",
            msg.OrderId, msg.Customer, msg.Amount, msg.Priority);
        return Task.CompletedTask;
    }
}
