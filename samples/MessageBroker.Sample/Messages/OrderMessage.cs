using MessageBroker.Core.Message;

namespace MessageBroker.Sample.Messages;

[MessageName("order.created")]
public sealed record OrderMessage(
    string OrderId = "",
    string Customer = "",
    decimal Amount = 0m,
    string Priority = "normal");
