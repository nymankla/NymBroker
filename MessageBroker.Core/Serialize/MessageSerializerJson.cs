using System.Collections.Concurrent;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Microsoft.IO;
using MessageBroker.Core.Message;

namespace MessageBroker.Core.Serialize;

public sealed class MessageSerializerJson : IMessageSerializer
{
    private static readonly RecyclableMemoryStreamManager StreamManager = new();

    internal static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        DefaultBufferSize = 4096
    };

    // Cache PropertyInfo lookups by context type to avoid repeated reflection on hot path.
    private static readonly ConcurrentDictionary<Type, (PropertyInfo? Prop, Type? MsgType)> PropCache = new();

    public Stream Serialize(IMessageContext context)
    {
        // RawMessageContext already carries the parsed payload — reuse it directly.
        var messageElement = context is RawMessageContext raw
            ? raw.RawMessage
            : ExtractMessageElement(context);

        var dto = new MessageContextDto
        {
            Id = context.Id,
            CorrelationId = context.CorrelationId,
            Address = context.Address,
            MessageType = context.MessageType,
            Created = context.Created,
            TraceParent = context.TraceParent,
            TraceState = context.TraceState,
            Message = messageElement
        };

        var ms = StreamManager.GetStream("MessageSerializerJson.Serialize");
        JsonSerializer.Serialize(ms, dto, JsonOptions);
        ms.Position = 0;
        return ms;
    }

    public IMessageContext Deserialize(string json)
    {
        var dto = JsonSerializer.Deserialize<MessageContextDto>(json, JsonOptions)
            ?? throw new InvalidOperationException("Failed to deserialize message context.");

        return new RawMessageContext
        {
            Id = dto.Id,
            CorrelationId = dto.CorrelationId,
            Address = dto.Address,
            MessageType = dto.MessageType,
            Created = dto.Created,
            TraceParent = dto.TraceParent,
            TraceState = dto.TraceState,
            RawMessage = dto.Message
        };
    }

    public IMessageContext Deserialize(Stream stream)
    {
        // Rent a buffer instead of reading the whole stream to a string.
        using var reader = new StreamReader(stream, Encoding.UTF8, leaveOpen: true);
        var json = reader.ReadToEnd();
        return Deserialize(json);
    }

    /// <summary>Typed deserialization of the message payload from an already-deserialized context.</summary>
    public static T? DeserializeMessage<T>(RawMessageContext ctx) where T : class
        => JsonSerializer.Deserialize<T>(ctx.RawMessage.GetRawText(), JsonOptions);

    public static object? DeserializeMessageObject(RawMessageContext ctx, Type messageType)
        => JsonSerializer.Deserialize(ctx.RawMessage.GetRawText(), messageType, JsonOptions);

    // --- helpers ---

    private static JsonElement ExtractMessageElement(IMessageContext context)
    {
        var contextType = context.GetType();
        var (prop, msgType) = PropCache.GetOrAdd(contextType, t =>
        {
            if (!t.IsGenericType || t.GetGenericTypeDefinition() != typeof(MessageContext<>))
                return (null, null);
            var p = t.GetProperty("Message");
            var m = t.GetGenericArguments()[0];
            return (p, m);
        });

        if (prop == null || msgType == null) return default;
        var value = prop.GetValue(context);
        return value == null ? default : JsonSerializer.SerializeToElement(value, msgType, JsonOptions);
    }
}
