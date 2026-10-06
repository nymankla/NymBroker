using System.Text.Json.Serialization;

namespace NymBroker.Core.Endpoint;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum EndpointMode
{
    ReadWrite,
    ReadOnly,
    WriteOnly
}
