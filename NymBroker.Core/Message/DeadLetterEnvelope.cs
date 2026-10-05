using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using NymBroker.Core.Serialize;

namespace NymBroker.Core.Message;

/// <summary>
/// Adds the <c>deadLetter</c> block to envelope bytes. The payload is parsed and re-serialized, so insignificant
/// whitespace is normalized; property values and order are kept (a new block is appended, an existing one replaced in place).
/// </summary>
public static class DeadLetterEnvelope
{
    public const int MaxDescriptionLength = 4096;

    private const string UndecodableMessageType = "nymbroker.undecodable";

    /// <summary>
    /// Returns the envelope bytes with the <c>deadLetter</c> block set (replacing any existing one); every other
    /// property is copied unchanged. <see cref="DeadLetterInfo.Description"/> is truncated to <see cref="MaxDescriptionLength"/> characters. When <paramref name="raw"/> is not a JSON object, a new
    /// <see cref="UndecodableMessage"/> envelope carrying the original bytes is returned instead.
    /// </summary>
    public static byte[] Annotate(ReadOnlySpan<byte> raw, DeadLetterInfo info)
    {
        ArgumentNullException.ThrowIfNull(info);
        info = Normalize(info);

        JsonObject? envelope = null;
        try { envelope = JsonNode.Parse(raw) as JsonObject; }
        catch (Exception ex) when (ex is JsonException or ArgumentException or InvalidOperationException) { }

        if (envelope == null)
            envelope = CreateUndecodableEnvelope(raw);

        envelope["deadLetter"] = JsonSerializer.SerializeToNode(info, MessageSerializerJson.JsonOptions);
        return JsonSerializer.SerializeToUtf8Bytes(envelope);
    }

    private static DeadLetterInfo Normalize(DeadLetterInfo info)
    {
        var description = info.Description is { Length: > MaxDescriptionLength }
            ? info.Description[..MaxDescriptionLength]
            : info.Description;
        var at = info.DeadLetteredAt.Kind == DateTimeKind.Utc ? info.DeadLetteredAt : info.DeadLetteredAt.ToUniversalTime();
        return info with { Description = description, DeadLetteredAt = at };
    }

    private static JsonObject CreateUndecodableEnvelope(ReadOnlySpan<byte> raw)
    {
        string? text = null;
        try { text = new UTF8Encoding(false, true).GetString(raw); }
        catch (DecoderFallbackException) { }

        var payload = new JsonObject { ["payloadBase64"] = Convert.ToBase64String(raw) };
        if (text != null) payload["payloadText"] = text;

        return new JsonObject
        {
            ["id"] = Guid.NewGuid(),
            ["correlationId"] = Guid.NewGuid(),
            ["messageType"] = UndecodableMessageType,
            ["created"] = DateTime.UtcNow,
            ["message"] = payload
        };
    }
}
