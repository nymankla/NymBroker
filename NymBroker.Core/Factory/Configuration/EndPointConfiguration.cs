using System.Text.Json;
using NymBroker.Core.Endpoint.File;

namespace NymBroker.Core.Factory.Configuration;

public sealed class EndPointConfiguration
{
    public string Name { get; set; } = string.Empty;
    public EndPointType Type { get; set; }

    /// <summary>Raw JSON config object — deserialized to the concrete settings type per <see cref="Type"/>.</summary>
    public JsonElement? Config { get; set; }

    public FileSettings ToFileSettings()
        => Config.HasValue
            ? JsonSerializer.Deserialize<FileSettings>(Config.Value.GetRawText(), Serialize.MessageSerializerJson.JsonOptions) ?? new()
            : new();
}
