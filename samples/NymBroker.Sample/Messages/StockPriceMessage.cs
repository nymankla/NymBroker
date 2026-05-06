using NymBroker.Core.Message;

namespace NymBroker.Sample.Messages;

[MessageName("stock.price")]
public sealed record StockPriceMessage(
    string Ticker = "",
    decimal Price = 0m,
    DateTime AsOf = default);
