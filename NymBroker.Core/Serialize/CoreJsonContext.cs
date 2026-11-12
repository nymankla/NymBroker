using System.Text.Json.Serialization;
using System.Text.Json;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Factory.Configuration;
using NymBroker.Core.Message;

namespace NymBroker.Core.Serialize;

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, PropertyNameCaseInsensitive = true)]
[JsonSerializable(typeof(BrokerConfiguration))]
[JsonSerializable(typeof(DeadLetterInfo))]
[JsonSerializable(typeof(SplitMessage))]
[JsonSerializable(typeof(UndecodableMessage))]
[JsonSerializable(typeof(EndpointAddress))]
[JsonSerializable(typeof(MessageContextDto<JsonElement>))]
internal partial class CoreJsonContext : JsonSerializerContext;
