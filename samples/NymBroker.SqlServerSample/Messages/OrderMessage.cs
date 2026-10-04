using NymBroker.Core.Message;

namespace NymBroker.SqlServerSample.Messages;

[MessageName("order.created")]
public sealed record OrderMessage(
    string OrderId   = "",
    string Customer  = "",
    decimal Amount   = 0m,
    string Priority  = "normal");
