namespace NymBroker.Core.Message;

/// <summary>
/// Payload of the envelope <see cref="DeadLetterEnvelope.Annotate"/> creates when the dead-lettered bytes are not a JSON object.
/// Handle it on a dead-letter endpoint with <c>IConsume&lt;UndecodableMessage&gt;</c>.
/// </summary>
[MessageName("nymbroker.undecodable")]
public sealed class UndecodableMessage
{
    /// <summary>The original bytes, Base64-encoded.</summary>
    public string PayloadBase64 { get; set; } = string.Empty;

    /// <summary>The original bytes as text, when they are valid UTF-8.</summary>
    public string? PayloadText { get; set; }
}
