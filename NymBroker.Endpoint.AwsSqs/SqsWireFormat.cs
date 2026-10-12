using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Unicode;
using Amazon.SQS.Model;

namespace NymBroker.Endpoint.AwsSqs;

/// <summary>
/// Bytes ↔ SQS message body. An SQS body is a string limited to XML-safe characters. NymBroker envelopes (UTF-8 JSON)
/// are sent as text, unchanged; anything else is Base64 with the <see cref="EncodingAttribute"/> message attribute, so
/// every byte[] round-trips.
/// </summary>
internal static class SqsWireFormat
{
    internal const string EncodingAttribute = "nymbroker-encoding";
    internal const string Base64 = "base64";

    /// <summary>Conservative per-request limit for SendMessageBatch, valid on every SQS-compatible server.</summary>
    internal const int MaxBatchBytes = 256 * 1024;
    internal const int MaxBatchEntries = 10;

    internal const string DefaultMessageGroupId = "nymbroker";

    internal static (string Body, bool IsBase64) Encode(byte[] bytes)
    {
        if (Utf8.IsValid(bytes))
        {
            var text = Encoding.UTF8.GetString(bytes);
            if (IsAllowedInSqs(text)) return (text, false);
        }
        return (Convert.ToBase64String(bytes), true);
    }

    internal static byte[] Decode(Message message)
        => message.MessageAttributes is { } attributes
           && attributes.TryGetValue(EncodingAttribute, out var encoding)
           && encoding.StringValue == Base64
            ? Convert.FromBase64String(message.Body)
            : Encoding.UTF8.GetBytes(message.Body);

    /// <summary>SQS accepts #x9 | #xA | #xD | #x20–#xD7FF | #xE000–#xFFFD | #x10000–#x10FFFF.</summary>
    internal static bool IsAllowedInSqs(string text)
    {
        foreach (var c in text)
        {
            if (c < 0x20 && c is not ('\t' or '\n' or '\r')) return false;
            if (c is '￾' or '￿') return false;
            // Surrogates only occur as valid pairs here (the input was valid UTF-8), i.e. #x10000–#x10FFFF.
        }
        return true;
    }

    internal static Dictionary<string, MessageAttributeValue>? Attributes(bool isBase64)
        => isBase64
            ? new() { [EncodingAttribute] = new MessageAttributeValue { DataType = "String", StringValue = Base64 } }
            : null;

    /// <summary>
    /// FIFO group and deduplication ids: the group from the setting or the envelope's <c>correlationId</c>, the
    /// deduplication id from the envelope's <c>id</c>. Bytes that are not an envelope use a SHA-256 of the body.
    /// </summary>
    internal static (string GroupId, string DeduplicationId) FifoIds(byte[] bytes, string? configuredGroupId)
    {
        string? id = null, correlationId = null;
        try
        {
            var reader = new Utf8JsonReader(bytes);
            if (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
            {
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    var isId = reader.ValueTextEquals("id"u8);
                    var isCorrelation = !isId && reader.ValueTextEquals("correlationId"u8);
                    reader.Read();
                    if ((isId || isCorrelation) && reader.TokenType == JsonTokenType.String)
                    {
                        if (isId) id = reader.GetString();
                        else correlationId = reader.GetString();
                    }
                    else
                        reader.Skip();
                }
            }
        }
        catch (JsonException)
        {
            // Not JSON: fall through to the hash and the default group.
        }

        return (configuredGroupId ?? NonEmpty(correlationId) ?? DefaultMessageGroupId,
                NonEmpty(id) ?? Convert.ToHexString(SHA256.HashData(bytes)));
    }

    private static string? NonEmpty(string? value)
        => string.IsNullOrEmpty(value) || value == Guid.Empty.ToString() ? null : value;

    /// <summary>Splits messages into SendMessageBatch requests of at most 10 entries and <see cref="MaxBatchBytes"/>, in order.</summary>
    internal static IEnumerable<List<(byte[] Original, string Body, bool IsBase64)>> Chunk(IReadOnlyList<byte[]> messages)
    {
        var chunk = new List<(byte[], string, bool)>(MaxBatchEntries);
        var size = 0;
        foreach (var message in messages)
        {
            var (body, isBase64) = Encode(message);
            var bytes = Encoding.UTF8.GetByteCount(body);
            if (chunk.Count == MaxBatchEntries || (chunk.Count > 0 && size + bytes > MaxBatchBytes))
            {
                yield return chunk;
                chunk = new List<(byte[], string, bool)>(MaxBatchEntries);
                size = 0;
            }
            chunk.Add((message, body, isBase64));
            size += bytes;
        }
        if (chunk.Count > 0) yield return chunk;
    }
}
