using NymBroker.Core.Message;

namespace NymBroker.RabbitSample.Messages;

[MessageName("order.created")]
public sealed record OrderMessage(string OrderId, string Customer, decimal Amount, string Priority);
