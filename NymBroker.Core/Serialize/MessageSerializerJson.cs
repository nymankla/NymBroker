using System.Text.Json;
using NymBroker.Core.Message;

namespace NymBroker.Core.Serialize;

public sealed class MessageSerializerJson : IMessageSerializer
{
    internal static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        DefaultBufferSize = 4096
    };

    public Stream Serialize(IMessageContext context) => new MemoryStream(SerializeToBytes(context));

    /// <summary>The envelope as UTF-8. A typed message is written in one pass; a parsed one reuses its payload.</summary>
    internal static byte[] SerializeToBytes(IMessageContext context) => context switch
    {
        RawMessageContext raw => Write(raw, raw.RawMessage),
        ITypedMessageContext typed when typed.HasMessage => typed.SerializeEnvelope(),
        _ => Write(context, default(JsonElement))
    };

    internal static byte[] Write<TMessage>(IMessageContext context, TMessage? message)
        => JsonSerializer.SerializeToUtf8Bytes(new MessageContextDto<TMessage>
        {
            Id = context.Id,
            CorrelationId = context.CorrelationId,
            Address = context.Address,
            MessageType = context.MessageType,
            Created = context.Created,
            Message = message,
            DeadLetter = context.DeadLetter
        }, JsonOptions);

    public IMessageContext Deserialize(string json)
        => ToRawContext(JsonSerializer.Deserialize<MessageContextDto<JsonElement>>(json, JsonOptions));

    public IMessageContext Deserialize(Stream stream)
        => ToRawContext(JsonSerializer.Deserialize<MessageContextDto<JsonElement>>(stream, JsonOptions));

    public IMessageContext Deserialize(ReadOnlySpan<byte> utf8Json)
        => ToRawContext(JsonSerializer.Deserialize<MessageContextDto<JsonElement>>(utf8Json, JsonOptions));

    /// <summary>Typed deserialization of the message payload from an already-deserialized context.</summary>
    public static T? DeserializeMessage<T>(RawMessageContext ctx) where T : class
        => ctx.RawMessage.Deserialize<T>(JsonOptions);

    public static object? DeserializeMessageObject(RawMessageContext ctx, Type messageType)
        => ctx.RawMessage.Deserialize(messageType, JsonOptions);

    private static RawMessageContext ToRawContext(MessageContextDto<JsonElement>? dto)
    {
        if (dto is null) throw new InvalidOperationException("Failed to deserialize message context.");

        return new RawMessageContext
        {
            Id = dto.Id,
            CorrelationId = dto.CorrelationId,
            Address = dto.Address,
            MessageType = dto.MessageType,
            Created = dto.Created,
            DeadLetter = dto.DeadLetter,
            RawMessage = dto.Message
        };
    }
}
