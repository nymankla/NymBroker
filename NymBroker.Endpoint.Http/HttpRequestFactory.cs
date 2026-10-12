using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace NymBroker.Endpoint.Http;

/// <summary>Builds the HTTP request for one message. Pure (apart from the clock passed in), so it is unit-tested directly.</summary>
internal static class HttpRequestFactory
{
    internal const string MessageIdHeader = "X-NymBroker-Message-Id";
    internal const string MessageTypeHeader = "X-NymBroker-Message-Type";
    internal const string CorrelationIdHeader = "X-NymBroker-Correlation-Id";
    internal const string IdempotencyKeyHeader = "Idempotency-Key";
    internal const string SignatureHeader = "X-NymBroker-Signature";

    /// <summary>The envelope fields the request needs; all null when the bytes are not a NymBroker envelope.</summary>
    internal sealed record EnvelopeInfo(string? Id, string? MessageType, string? CorrelationId, byte[]? Payload);

    /// <summary>
    /// Reads <c>id</c>, <c>messageType</c>, <c>correlationId</c> and the raw <c>message</c> of an envelope. Bytes that are
    /// not a JSON object with a <c>message</c> property (e.g. raw input posted with <c>PostRawAsync</c>) give an empty result.
    /// </summary>
    internal static EnvelopeInfo ReadEnvelope(byte[] bytes)
    {
        try
        {
            using var doc = JsonDocument.Parse(bytes);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object || !root.TryGetProperty("message", out var message))
                return Empty;
            return new EnvelopeInfo(
                StringOrNull(root, "id"),
                StringOrNull(root, "messageType"),
                StringOrNull(root, "correlationId"),
                Encoding.UTF8.GetBytes(message.GetRawText()));
        }
        catch (JsonException)
        {
            return Empty;
        }
    }

    private static readonly EnvelopeInfo Empty = new(null, null, null, null);

    private static string? StringOrNull(JsonElement root, string name)
        => root.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String && value.GetString() is { Length: > 0 } s ? s : null;

    internal static HttpRequestMessage Create(HttpSettings settings, byte[] message, DateTimeOffset now)
    {
        var envelope = ReadEnvelope(message);
        var isEnvelope = envelope.Payload is not null;
        var body = settings.BodyFormat == HttpBodyFormat.Payload && isEnvelope ? envelope.Payload! : message;

        var request = new HttpRequestMessage(new HttpMethod(settings.Method.ToUpperInvariant()), settings.Url)
        {
            Content = new ByteArrayContent(body)
        };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue(isEnvelope || LooksLikeJson(body) ? "application/json" : "application/octet-stream");

        foreach (var (name, value) in settings.Headers)
        {
            if (!request.Headers.TryAddWithoutValidation(name, value))
                request.Content.Headers.TryAddWithoutValidation(name, value);   // e.g. a custom Content-* header
        }

        if (envelope.Id is { } id)
        {
            request.Headers.TryAddWithoutValidation(MessageIdHeader, id);
            request.Headers.TryAddWithoutValidation(IdempotencyKeyHeader, id);
        }
        if (envelope.MessageType is { } type)
            request.Headers.TryAddWithoutValidation(MessageTypeHeader, type);
        if (envelope.CorrelationId is { } correlation)
            request.Headers.TryAddWithoutValidation(CorrelationIdHeader, correlation);

        if (settings.SigningSecret is { } secret)
            request.Headers.TryAddWithoutValidation(SignatureHeader, Sign(secret, now.ToUnixTimeSeconds(), body));

        return request;
    }

    /// <summary><c>t=&lt;timestamp&gt;,v1=&lt;lowercase hex HMAC-SHA256(secret, "&lt;timestamp&gt;." + body)&gt;</c>.</summary>
    internal static string Sign(string secret, long timestamp, byte[] body)
    {
        var prefix = Encoding.UTF8.GetBytes($"{timestamp}.");
        var signed = new byte[prefix.Length + body.Length];
        prefix.CopyTo(signed, 0);
        body.CopyTo(signed, prefix.Length);
        var mac = HMACSHA256.HashData(Encoding.UTF8.GetBytes(secret), signed);
        return $"t={timestamp},v1={Convert.ToHexStringLower(mac)}";
    }

    private static bool LooksLikeJson(byte[] body)
    {
        try
        {
            using var _ = JsonDocument.Parse(body);
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }
}
