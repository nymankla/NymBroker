using System.Globalization;
using System.Text.Json;
using NymBroker.Core.Serialize;
using Microsoft.Extensions.Configuration;

namespace NymBroker.Core.Factory.Configuration;

public static class BrokerConfigurationReader
{
    private const string DefaultSectionKey = "NymBroker";

    /// <summary>
    /// Reads broker configuration directly from a JSON file using System.Text.Json,
    /// which correctly preserves <see cref="EndPointConfiguration.Config"/> as a live JsonElement.
    /// </summary>
    public static BrokerConfiguration Read(string filePath, string sectionKey = DefaultSectionKey)
    {
        var json = File.ReadAllText(filePath);
        using var doc = JsonDocument.Parse(json);
        if (!doc.RootElement.TryGetProperty(sectionKey, out var section))
            return new();
        return JsonSerializer.Deserialize(section, CoreJsonContext.Default.BrokerConfiguration) ?? new();
    }

    /// <summary>
    /// Reads broker configuration from an <see cref="IConfiguration"/> section.
    /// Note: the endpoint <c>Config</c> object is re-serialized to JSON so that
    /// <see cref="EndPointConfiguration.Config"/> contains a valid JsonElement.
    /// </summary>
    public static BrokerConfiguration Read(IConfiguration configuration, string sectionKey = DefaultSectionKey)
    {
        var section = configuration.GetSection(sectionKey);
        // Re-serialize the section to JSON so JsonElement? Config survives the round-trip.
        var json = SerializeSectionToJson(section);
        using var doc = JsonDocument.Parse(json);
        return JsonSerializer.Deserialize(doc.RootElement, CoreJsonContext.Default.BrokerConfiguration) ?? new();
    }

    private static string SerializeSectionToJson(IConfigurationSection section)
    {
        using var ms = new MemoryStream();
        using var writer = new Utf8JsonWriter(ms);
        WriteSection(writer, section);
        writer.Flush();
        return System.Text.Encoding.UTF8.GetString(ms.ToArray());
    }

    private static void WriteSection(Utf8JsonWriter writer, IConfigurationSection section)
    {
        var children = section.GetChildren().ToList();

        if (children.Count == 0)
            WriteValue(writer, section.Value);
        else if (children.All(c => int.TryParse(c.Key, CultureInfo.InvariantCulture, out _)))
            WriteArray(writer, children);
        else
            WriteObject(writer, children);
    }

    /// <summary>A leaf: configuration holds only strings, so a value that parses as a bool or number is written as one.</summary>
    private static void WriteValue(Utf8JsonWriter writer, string? value)
    {
        if (value is null)
            writer.WriteNullValue();
        else if (bool.TryParse(value, out var b))
            writer.WriteBooleanValue(b);
        else if (long.TryParse(value, out var l))
            writer.WriteNumberValue(l);
        else if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
            writer.WriteNumberValue(d);
        else
            writer.WriteStringValue(value);
    }

    private static void WriteArray(Utf8JsonWriter writer, List<IConfigurationSection> children)
    {
        writer.WriteStartArray();
        foreach (var child in children.OrderBy(c => int.Parse(c.Key, CultureInfo.InvariantCulture)))
            WriteSection(writer, child);
        writer.WriteEndArray();
    }

    private static void WriteObject(Utf8JsonWriter writer, List<IConfigurationSection> children)
    {
        writer.WriteStartObject();
        foreach (var child in children)
        {
            writer.WritePropertyName(child.Key);
            WriteSection(writer, child);
        }
        writer.WriteEndObject();
    }
}
