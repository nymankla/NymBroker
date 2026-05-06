using NymBroker.ConfigSample.Messages;
using NymBroker.Core.Consume;
using NymBroker.Core.Message;
using Microsoft.Extensions.Logging;

namespace NymBroker.ConfigSample.Consumers;

public sealed class OrderConsumer : IConsume<OrderMessage>
{
    private readonly ILogger<OrderConsumer> _logger;

    public string Name => nameof(OrderConsumer);

    public OrderConsumer(ILogger<OrderConsumer> logger) => _logger = logger;

    public Task ConsumeAsync(OrderMessage message, IMessageContext context, CancellationToken ct = default)
    {
        _logger.LogInformation(
            "[OrderConsumer] {OrderId} — {Customer} £{Amount} (priority: {Priority})",
            message.OrderId, message.Customer, message.Amount, message.Priority);
        return Task.CompletedTask;
    }
}
