using NymBroker.Core.Serialize;

namespace NymBroker.Core.Transform;

/// <summary>
/// The counterpart of <see cref="IInputTransformer"/>: converts each message sent to one endpoint into that
/// destination's own format (XML, CSV, a partner's JSON schema) just before it reaches the transport.
/// </summary>
public interface IOutputTransformer
{
    /// <summary>
    /// Transforms an outgoing message into the bytes to send instead of the NymBroker envelope.
    /// Return null to send nothing. An exception fails the send, like a failing endpoint.
    /// </summary>
    byte[]? Transform(RawMessageContext message, string destinationEndpoint);
}
