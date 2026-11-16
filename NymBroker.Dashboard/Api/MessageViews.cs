using System.Text;
using System.Text.Json;
using NymBroker.Core.Endpoint.Queue;

namespace NymBroker.Dashboard.Api;

/// <summary>Turns a queued message into what the queue view shows.</summary>
internal static class MessageViews
{
    /// <summary>
    /// The envelope fields are shown always (ids, type, times, dead-letter reason); the payload only when
    /// <paramref name="showPayload"/>, after <see cref="NymBrokerDashboardOptions.RedactPayload"/> and cut to
    /// <see cref="NymBrokerDashboardOptions.MaxPayloadBytes"/>.
    /// </summary>
    public static MessageView From(QueueMessageInfo message, string endpoint, bool showPayload, NymBrokerDashboardOptions options)
    {
        string? payload = null;
        var truncated = false;
        if (showPayload)
        {
            var shown = options.RedactPayload is { } redact ? redact(endpoint, message.Payload) : message.Payload;
            (payload, truncated) = AsText(shown.Span, options.MaxPayloadBytes);
        }

        return new MessageView(message.Id, message.State, message.Attempts, message.CreatedAt, message.DeliverAt, message.FailedAt,
            message.LastError, message.Payload.Length, ReadEnvelope(message.Payload), payload, truncated);
    }

    /// <summary>UTF-8 text, cut to <paramref name="maxBytes"/>; bytes that are not text are shown as Base64.</summary>
    internal static (string Text, bool Truncated) AsText(ReadOnlySpan<byte> bytes, int maxBytes)
    {
        var truncated = bytes.Length > maxBytes;
        var shown = truncated ? bytes[..maxBytes] : bytes;
        return (System.Text.Unicode.Utf8.IsValid(shown) || truncated ? Encoding.UTF8.GetString(shown) : Convert.ToBase64String(shown), truncated);
    }

    /// <summary>
    /// The envelope's fields, or null when the bytes are not a NymBroker envelope: raw input for an input transformer,
    /// or anything else a producer wrote. That is a normal case, not an error, so a parse failure just returns null.
    /// </summary>
    internal static EnvelopeView? ReadEnvelope(ReadOnlyMemory<byte> bytes)
    {
        try
        {
            using var document = JsonDocument.Parse(bytes);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty("messageType", out _)) return null;
            var address = root.TryGetProperty("address", out var a) && a.ValueKind == JsonValueKind.Object ? a : default;
            return new EnvelopeView(Text(root, "id"), Text(root, "correlationId"), Text(root, "messageType"), Text(root, "created"),
                Text(address, "from"), Text(address, "to"), Text(root, "traceParent"), ReadDeadLetter(root));
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static DeadLetterView? ReadDeadLetter(JsonElement root)
    {
        if (!root.TryGetProperty("deadLetter", out var d) || d.ValueKind != JsonValueKind.Object) return null;
        int? deliveryCount = d.TryGetProperty("deliveryCount", out var count) && count.TryGetInt32(out var n) ? n : null;
        return new DeadLetterView(Text(d, "reason"), Text(d, "description"), Text(d, "exceptionType"), Text(d, "sourceEndpoint"),
            Text(d, "deadLetteredAt"), deliveryCount);
    }

    private static string? Text(JsonElement element, string name)
        => element.ValueKind == JsonValueKind.Object && element.TryGetProperty(name, out var value) && value.ValueKind is not (JsonValueKind.Null or JsonValueKind.Undefined)
            ? value.ValueKind == JsonValueKind.String ? value.GetString() : value.GetRawText()
            : null;
}
