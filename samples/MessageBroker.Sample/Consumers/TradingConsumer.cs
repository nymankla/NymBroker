using MessageBroker.Core.Consume;
using MessageBroker.Core.Message;
using MessageBroker.Sample.Messages;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Sample.Consumers;

/// <summary>
/// Single consumer that handles both OrderMessage and StockPriceMessage.
/// Register once with AddConsumer&lt;TradingConsumer&gt;() — the builder
/// discovers and registers all IConsume&lt;T&gt; interfaces automatically.
/// </summary>
public sealed class TradingConsumer : IConsume<OrderMessage>, IConsume<StockPriceMessage>
{
    private readonly ILogger<TradingConsumer> _logger;

    public string Name => nameof(TradingConsumer);

    public TradingConsumer(ILogger<TradingConsumer> logger) => _logger = logger;

    public Task ConsumeAsync(OrderMessage message, IMessageContext context, CancellationToken ct = default)
    {
        _logger.LogInformation(
            "[TradingConsumer] Order {OrderId} — {Customer} £{Amount} ({Priority})",
            message.OrderId, message.Customer, message.Amount, message.Priority);
        return Task.CompletedTask;
    }

    public Task ConsumeAsync(StockPriceMessage message, IMessageContext context, CancellationToken ct = default)
    {
        _logger.LogInformation(
            "[TradingConsumer] Quote {Ticker} = {Price:C} at {AsOf:HH:mm:ss}",
            message.Ticker, message.Price, message.AsOf);
        return Task.CompletedTask;
    }
}
