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

    /// <summary>
    /// The W3C <c>traceparent</c> of the activity that sent the message (envelope <c>traceParent</c>); null when the
    /// sender had none. The consumer's <c>nymbroker.process</c> activity continues it.
    /// </summary>
    string? TraceParent => null;

    /// <summary>The W3C <c>tracestate</c> sent with <see cref="TraceParent"/> (envelope <c>traceState</c>); null when there is none.</summary>
    string? TraceState => null;
}

public interface IMessageContext<T> : IMessageContext where T : class
{
    T? Message { get; set; }
}
