using System.Text.Json.Serialization;

namespace NymBroker.Core.Factory;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum EndPointType
{
    File,
    RabbitMq,
    Memory,
    Sql,
    Postgres
}
