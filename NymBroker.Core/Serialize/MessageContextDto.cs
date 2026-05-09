using System.Text.Json;
using System.Text.Json.Serialization;
using NymBroker.Core.Message;

namespace NymBroker.Core.Serialize;

internal sealed class MessageContextDto
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
    public JsonElement Message { get; set; }
}

/// <summary>Returned by the deserializer; carries the raw message payload for deferred typed deserialization.</summary>
public sealed class RawMessageContext : IMessageContext
{
    public Guid Id { get; set; }
    public Guid CorrelationId { get; set; }
    public EndpointAddress? Address { get; set; }
    public string? MessageType { get; set; }
    public DateTime Created { get; set; }
    public JsonElement RawMessage { get; set; }
}
