using NymBroker.Core.Consume;
using NymBroker.Core.Message;
using NymBroker.Sample.Messages;
using Microsoft.Extensions.Logging;

namespace NymBroker.Sample.Consumers;

public sealed class StockPriceConsumer : IConsume<StockPriceMessage>
{
    private readonly ILogger<StockPriceConsumer> _logger;

    public StockPriceConsumer(ILogger<StockPriceConsumer> logger) => _logger = logger;

    public Task ConsumeAsync(StockPriceMessage message, IMessageContext context, CancellationToken ct = default)
    {
        _logger.LogInformation(
            "[StockPriceConsumer] {Ticker} = {Price:C} at {AsOf:HH:mm:ss UTC}",
            message.Ticker, message.Price, message.AsOf);
        return Task.CompletedTask;
    }
}
