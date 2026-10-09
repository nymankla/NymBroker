using System.Text.Json;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.File;

namespace NymBroker.Core.Factory.Configuration;

public sealed class EndPointConfiguration
{
    public string Name { get; set; } = string.Empty;
    /// <summary>
    /// Endpoint type name — one of <see cref="EndPointType"/> or a name defined by an extension package.
    /// Unknown types are loaded as-is and left for the matching <c>With*()</c> extension to process.
    /// </summary>
    public string Type { get; set; } = string.Empty;
    public EndpointMode Mode { get; set; } = EndpointMode.ReadWrite;

    /// <summary>Case-insensitive check of <see cref="Type"/> against an endpoint type name.</summary>
    public bool IsType(string type) => string.Equals(Type, type, StringComparison.OrdinalIgnoreCase);

    /// <summary>Raw JSON config object — deserialized to the concrete settings type per <see cref="Type"/>.</summary>
    public JsonElement? Config { get; set; }

    public FileSettings ToFileSettings() => GetSettings<FileSettings>();

    /// <summary>
    /// Deserializes <see cref="Config"/> (camelCase, case-insensitive) into an endpoint's settings type; a missing
    /// <c>Config</c> gives the type's defaults. Used by the transport packages' <c>With…()</c> extensions.
    /// </summary>
    public T GetSettings<T>() where T : class, new()
        => Config.HasValue
            ? JsonSerializer.Deserialize<T>(Config.Value.GetRawText(), Serialize.MessageSerializerJson.JsonOptions) ?? new()
            : new();
}
