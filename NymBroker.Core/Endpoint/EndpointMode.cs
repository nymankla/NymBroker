using System.Text.Json.Serialization;

namespace NymBroker.Core.Endpoint;

[JsonConverter(typeof(JsonStringEnumConverter<EndpointMode>))]
public enum EndpointMode
{
    ReadWrite,
    ReadOnly,
    WriteOnly
}
