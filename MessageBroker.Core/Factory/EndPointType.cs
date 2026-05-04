using System.Text.Json.Serialization;

namespace MessageBroker.Core.Factory;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum EndPointType
{
    File,
    RabbitMq,
    Memory
}
