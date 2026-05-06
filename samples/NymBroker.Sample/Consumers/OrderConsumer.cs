using NymBroker.Core.Consume;
using NymBroker.Core.Message;
using NymBroker.Sample.Messages;
using Microsoft.Extensions.Logging;

namespace NymBroker.Sample.Consumers;

public sealed class OrderConsumer : IConsume<OrderMessage>
{
    private readonly ILogger<OrderConsumer> _logger;

    public string Name => nameof(OrderConsumer);

    public OrderConsumer(ILogger<OrderConsumer> logger) => _logger = logger;

    public Task ConsumeAsync(OrderMessage message, IMessageContext context, CancellationToken ct = default)
    {
        _logger.LogInformation(
            "[OrderConsumer] Order {OrderId} from {Customer} — £{Amount} (priority: {Priority})",
            message.OrderId, message.Customer, message.Amount, message.Priority);
        return Task.CompletedTask;
    }
}
