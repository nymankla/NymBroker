namespace NymBroker.Core.Message;

public interface IMessageContext
{
    Guid Id { get; }
    Guid CorrelationId { get; set; }
    EndpointAddress? Address { get; set; }
    string? MessageType { get; set; }
    DateTime Created { get; set; }
    string? TraceParent { get; set; }
    string? TraceState { get; set; }
}

public interface IMessageContext<T> : IMessageContext where T : class
{
    T? Message { get; set; }
}
