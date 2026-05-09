using NymBroker.Core.Message;

namespace NymBroker.Core.Message;

public sealed class MessageContext<T> : IMessageContext<T> where T : class
{
    public Guid Id { get; } = Guid.NewGuid();
    public Guid CorrelationId { get; set; } = Guid.NewGuid();
    public EndpointAddress? Address { get; set; }
    public string? MessageType { get; set; } = MessageTypeName.Get(typeof(T));
    public DateTime Created { get; set; } = DateTime.UtcNow;
    public T? Message { get; set; }
}
