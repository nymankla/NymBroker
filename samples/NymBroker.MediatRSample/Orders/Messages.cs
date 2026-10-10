using NymBroker.Core.Message;

namespace NymBroker.MediatRSample.Orders;

// MediatR: IRequest (a command, no response). NymBroker: a message posted to an endpoint, handled by one IConsume<T>.
// The id is chosen by the caller, so it can query the result later — a command returns no value.
[MessageName("orders.create-order")]
public sealed record CreateOrder(Guid OrderId, string Customer, decimal Amount);

// MediatR: INotification. NymBroker: a message published to a topic, handled by any number of ISubscribe<T>.
[MessageName("orders.order-created")]
public sealed record OrderCreated(Guid OrderId, string Customer, decimal Amount, DateTimeOffset CreatedAt);
