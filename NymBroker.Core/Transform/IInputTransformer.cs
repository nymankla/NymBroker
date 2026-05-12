using NymBroker.Core.Serialize;

namespace NymBroker.Core.Transform;

public interface IInputTransformer
{
    /// <summary>
    /// Transforms raw bytes from an endpoint into a message context.
    /// Return null to drop the message silently.
    /// </summary>
    RawMessageContext? Transform(ReadOnlySpan<byte> input, string? sourceEndpoint);
}
