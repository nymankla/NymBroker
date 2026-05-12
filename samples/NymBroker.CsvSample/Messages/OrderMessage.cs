using NymBroker.Core.Message;

namespace NymBroker.CsvSample.Messages;

[MessageName("order.created")]
public record OrderMessage(string OrderId, string Customer, decimal Amount, string Priority);
