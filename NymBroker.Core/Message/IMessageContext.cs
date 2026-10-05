namespace NymBroker.Core.Message;

public interface IMessageContext
{
    Guid Id { get; }
    Guid CorrelationId { get; set; }
    EndpointAddress? Address { get; set; }
    string? MessageType { get; set; }
    DateTime Created { get; set; }

    /// <summary>Why the message was dead-lettered; null for messages that were not.</summary>
    DeadLetterInfo? DeadLetter => null;
}

public interface IMessageContext<T> : IMessageContext where T : class
{
    T? Message { get; set; }
}
