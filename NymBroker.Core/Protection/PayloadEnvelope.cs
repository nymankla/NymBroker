using System.Buffers.Text;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Serialize;

namespace NymBroker.Core.Protection;

/// <summary>The envelope's <c>protection</c> block: present only on an envelope whose <c>message</c> is protected.</summary>
internal sealed class PayloadProtectionInfo
{
    [JsonPropertyName("alg")]
    public string? Algorithm { get; set; }

    [JsonPropertyName("kid")]
    public string? KeyId { get; set; }
}

/// <summary>Protection could not be applied to or removed from a received envelope; <see cref="Reason"/> is the dead-letter reason.</summary>
internal sealed class PayloadProtectionException(string reason, string message, Exception? inner = null) : Exception(message, inner)
{
    public string Reason { get; } = reason;
}

/// <summary>Protects and unprotects the <c>message</c> field of envelope bytes; every other property is kept as it is.</summary>
internal static class PayloadEnvelope
{
    private const string ProtectionProperty = "protection";
    private const string MessageProperty = "message";

    /// <summary>
    /// The envelope with its <c>message</c> encrypted and a <c>protection</c> block in front of it. An envelope that is
    /// protected already (a wire-tapped or dead-lettered copy of a protected message) is returned unchanged.
    /// </summary>
    /// <exception cref="InvalidOperationException">The bytes are not a NymBroker envelope.</exception>
    public static byte[] Protect(byte[] envelope, IPayloadProtector protector, string endpointName)
    {
        JsonDocument document;
        try { document = JsonDocument.Parse(envelope); }
        catch (JsonException ex) { throw NotAnEnvelope(endpointName, ex); }

        using (document)
        {
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty(MessageProperty, out var message))
                throw NotAnEnvelope(endpointName, null);
            if (root.TryGetProperty(ProtectionProperty, out _))
                return envelope;

            var associatedData = AssociatedData(ReadGuid(root, "id", endpointName), ReadGuid(root, "correlationId", endpointName),
                root.TryGetProperty("messageType", out var type) && type.ValueKind == JsonValueKind.String ? type.GetString() : null);
            var protectedPayload = protector.Protect(JsonMarshal.GetRawUtf8Value(message), associatedData);
            return Write(root, protectedPayload);
        }
    }

    /// <summary>Replaces a protected payload in <paramref name="context"/> with the decrypted message.</summary>
    /// <exception cref="PayloadProtectionException">It cannot be decrypted.</exception>
    public static void Unprotect(RawMessageContext context, IPayloadProtector? protector)
    {
        var info = context.Protection!;
        if (protector is null)
            throw new PayloadProtectionException(DeadLetterReasons.PayloadUnprotectFailed,
                $"The payload is protected ({info.Algorithm}, key '{info.KeyId}') but this broker has no payload protector (ProtectPayloads).");
        if (info.Algorithm is null || info.KeyId is null || context.RawMessage.ValueKind != JsonValueKind.String)
            throw new PayloadProtectionException(DeadLetterReasons.PayloadUnprotectFailed, "The envelope's protection block or protected message is malformed.");

        context.RawMessage = ParseMessage(Decrypt(context, protector, info.Algorithm, info.KeyId));
        context.Protection = null;
    }

    private static byte[] Decrypt(RawMessageContext context, IPayloadProtector protector, string algorithm, string keyId)
    {
        try
        {
            var data = context.RawMessage.GetBytesFromBase64();
            return protector.Unprotect(new ProtectedPayload(algorithm, keyId, data),
                AssociatedData(context.Id, context.CorrelationId, context.MessageType));
        }
        catch (Exception ex)
        {
            throw new PayloadProtectionException(DeadLetterReasons.PayloadUnprotectFailed,
                $"The payload could not be decrypted ({algorithm}, key '{keyId}'): {ex.Message}", ex);
        }
    }

    private static JsonElement ParseMessage(byte[] plaintext)
    {
        try
        {
            using var document = JsonDocument.Parse(plaintext);
            return document.RootElement.Clone();
        }
        catch (JsonException ex)
        {
            throw new PayloadProtectionException(DeadLetterReasons.PayloadUnprotectFailed, "The decrypted payload is not JSON.", ex);
        }
    }

    /// <summary>Binds a ciphertext to its envelope: message id, correlation id and message type.</summary>
    internal static byte[] AssociatedData(Guid id, Guid correlationId, string? messageType)
    {
        var typeLength = messageType is null ? 0 : Encoding.UTF8.GetByteCount(messageType);
        var data = new byte[32 + typeLength];
        id.TryWriteBytes(data.AsSpan(0, 16));
        correlationId.TryWriteBytes(data.AsSpan(16, 16));
        if (messageType is not null) Encoding.UTF8.GetBytes(messageType, data.AsSpan(32));
        return data;
    }

    private static Guid ReadGuid(JsonElement root, string name, string endpointName)
        => root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String && value.TryGetGuid(out var guid)
            ? guid
            : throw NotAnEnvelope(endpointName, null);

    private static byte[] Write(JsonElement root, ProtectedPayload payload)
    {
        using var stream = new MemoryStream(Base64.GetMaxEncodedToUtf8Length(payload.Data.Length) + 512);
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            foreach (var property in root.EnumerateObject())
            {
                if (!property.NameEquals(MessageProperty))
                {
                    property.WriteTo(writer);
                    continue;
                }
                writer.WriteStartObject(ProtectionProperty);
                writer.WriteString("alg", payload.Algorithm);
                writer.WriteString("kid", payload.KeyId);
                writer.WriteEndObject();
                writer.WriteBase64String(MessageProperty, payload.Data);
            }
            writer.WriteEndObject();
        }
        return stream.ToArray();
    }

    private static InvalidOperationException NotAnEnvelope(string endpointName, Exception? inner)
        => new($"Endpoint '{endpointName}' protects payloads, which needs a NymBroker envelope (with id, correlationId and message); the bytes sent to it are not one.", inner);
}
