using System.Text.Json;
using System.Text.Json.Serialization;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Message;

namespace NymBroker.Core.Serialize;

internal sealed class MessageContextDto<TMessage>
{
    [JsonPropertyName("id")]
    public Guid Id { get; set; }

    [JsonPropertyName("correlationId")]
    public Guid CorrelationId { get; set; }

    [JsonPropertyName("address")]
    public EndpointAddress? Address { get; set; }

    [JsonPropertyName("messageType")]
    public string? MessageType { get; set; }

    [JsonPropertyName("created")]
    public DateTime Created { get; set; }

    [JsonPropertyName("message")]
    public TMessage? Message { get; set; }

    [JsonPropertyName("deadLetter")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DeadLetterInfo? DeadLetter { get; set; }

    [JsonPropertyName("traceParent")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? TraceParent { get; set; }

    [JsonPropertyName("traceState")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? TraceState { get; set; }

    [JsonPropertyName("protection")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Protection.PayloadProtectionInfo? Protection { get; set; }
}

/// <summary>Returned by the deserializer; carries the raw message payload for deferred typed deserialization.</summary>
public sealed class RawMessageContext : IMessageContext, ITraceCarrier
{
    public Guid Id { get; set; }
    public Guid CorrelationId { get; set; }
    public EndpointAddress? Address { get; set; }
    public string? MessageType { get; set; }
    public DateTime Created { get; set; }
    public DeadLetterInfo? DeadLetter { get; set; }

    /// <summary>
    /// The sender's W3C <c>traceparent</c>; see <see cref="IMessageContext.TraceParent"/>. The deserializer keeps only a
    /// valid one. An input transformer may set it from the external message's own trace header.
    /// </summary>
    public string? TraceParent { get; set; }

    public string? TraceState { get; set; }

    /// <summary>
    /// The payload as JSON. For a protected envelope this is the Base64 ciphertext until the broker has decrypted it on
    /// receive; consumers, routes and filters always see the decrypted message.
    /// </summary>
    public JsonElement RawMessage { get; set; }

    /// <summary>Set while <see cref="RawMessage"/> is still protected; cleared when the broker decrypts it.</summary>
    internal Protection.PayloadProtectionInfo? Protection { get; set; }
}
