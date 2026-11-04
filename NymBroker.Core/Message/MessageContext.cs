using NymBroker.Core.Serialize;

namespace NymBroker.Core.Message;

/// <summary>A context that can write its own envelope, so the serializer needs no reflection to reach the message.</summary>
internal interface ITypedMessageContext
{
    bool HasMessage { get; }
    byte[] SerializeEnvelope();
}

public sealed class MessageContext<T> : IMessageContext<T>, ITypedMessageContext where T : class
{
    public Guid Id { get; } = Guid.NewGuid();
    public Guid CorrelationId { get; set; } = Guid.NewGuid();
    public EndpointAddress? Address { get; set; }
    public string? MessageType { get; set; } = MessageTypeName.Get(typeof(T));
    public DateTime Created { get; set; } = DateTime.UtcNow;
    public DeadLetterInfo? DeadLetter { get; set; }
    public T? Message { get; set; }

    bool ITypedMessageContext.HasMessage => Message is not null;
    byte[] ITypedMessageContext.SerializeEnvelope() => MessageSerializerJson.Write(this, Message);
}
