using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using System.Diagnostics.CodeAnalysis;
using NymBroker.Core.Message;

namespace NymBroker.Core.Serialize;

public sealed class MessageSerializerJson : IMessageSerializer
{
    internal static readonly JsonSerializerOptions JsonOptions = CreateOptions([]);
    private static readonly MessageSerializerJson Default = new();
    private readonly JsonSerializerOptions _options;

    internal MessageSerializerJson(IEnumerable<JsonSerializerContext> contexts)
        => _options = CreateOptions(contexts);

    public MessageSerializerJson() : this([]) { }

    private static JsonSerializerOptions CreateOptions(IEnumerable<JsonSerializerContext> contexts)
    {
        var resolvers = new List<IJsonTypeInfoResolver> { CoreJsonContext.Default };
        resolvers.AddRange(contexts);
        if (JsonSerializer.IsReflectionEnabledByDefault)
            resolvers.Add(new DefaultJsonTypeInfoResolver());

        return new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true,
            DefaultBufferSize = 4096,
            TypeInfoResolver = JsonTypeInfoResolver.Combine(resolvers.ToArray())
        };
    }

    public Stream Serialize(IMessageContext context) => new MemoryStream(SerializeRegistered(context));

    /// <summary>The envelope as UTF-8. A typed message is written in one pass; a parsed one reuses its payload.</summary>
    internal static byte[] SerializeToBytes(IMessageContext context) => Default.SerializeRegistered(context);

    internal byte[] SerializeRegistered(IMessageContext context) => context switch
    {
        RawMessageContext raw => WriteRegistered(raw, raw.RawMessage),
        ITypedMessageContext typed when typed.HasMessage => typed.SerializeEnvelope(this),
        _ => WriteRegistered(context, default(JsonElement))
    };

    internal static byte[] Write<TMessage>(IMessageContext context, TMessage? message)
        => Default.WriteRegistered(context, message);

    internal byte[] WriteRegistered<TMessage>(IMessageContext context, TMessage? message)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            writer.WriteString("id", context.Id);
            writer.WriteString("correlationId", context.CorrelationId);
            WriteValue(writer, "address", context.Address, typeof(EndpointAddress));
            writer.WriteString("messageType", context.MessageType);
            writer.WriteString("created", context.Created);
            writer.WritePropertyName("message");
            JsonSerializer.Serialize(writer, message, _options.GetTypeInfo(typeof(TMessage)));
            if (context.DeadLetter is not null)
                WriteValue(writer, "deadLetter", context.DeadLetter, typeof(DeadLetterInfo));
            writer.WriteEndObject();
        }
        return stream.ToArray();
    }

    private void WriteValue<T>(Utf8JsonWriter writer, string name, T? value, Type type)
    {
        writer.WritePropertyName(name);
        JsonSerializer.Serialize(writer, value, _options.GetTypeInfo(type));
    }

    public IMessageContext Deserialize(string json)
        => ToRawContext((MessageContextDto<JsonElement>?)JsonSerializer.Deserialize(json, _options.GetTypeInfo(typeof(MessageContextDto<JsonElement>))));

    public IMessageContext Deserialize(Stream stream)
        => ToRawContext((MessageContextDto<JsonElement>?)JsonSerializer.Deserialize(stream, _options.GetTypeInfo(typeof(MessageContextDto<JsonElement>))));

    public IMessageContext Deserialize(ReadOnlySpan<byte> utf8Json)
        => ToRawContext((MessageContextDto<JsonElement>?)JsonSerializer.Deserialize(utf8Json, _options.GetTypeInfo(typeof(MessageContextDto<JsonElement>))));

    /// <summary>Typed deserialization of the message payload from an already-deserialized context using reflection-based metadata.</summary>
    [RequiresUnreferencedCode("Use the overload that accepts JsonTypeInfo<T> in trimmed or Native AOT applications.")]
    [RequiresDynamicCode("Use the overload that accepts JsonTypeInfo<T> in Native AOT applications.")]
    public static T? DeserializeMessage<T>(RawMessageContext ctx) where T : class
        => ctx.RawMessage.Deserialize<T>(JsonOptions);

    /// <summary>Typed deserialization using generated metadata, including in trimmed and Native AOT applications.</summary>
    public static T? DeserializeMessage<T>(RawMessageContext ctx, JsonTypeInfo<T> typeInfo) where T : class
        => ctx.RawMessage.Deserialize(typeInfo);

    [RequiresUnreferencedCode("This runtime-Type overload requires reflection-based JSON metadata.")]
    [RequiresDynamicCode("This runtime-Type overload may require dynamic code generation.")]
    public static object? DeserializeMessageObject(RawMessageContext ctx, Type messageType)
        => ctx.RawMessage.Deserialize(messageType, JsonOptions);

    internal object? DeserializeMessageObjectRegistered(RawMessageContext ctx, Type messageType)
        => ctx.RawMessage.Deserialize(_options.GetTypeInfo(messageType));

    internal T? DeserializeMessageRegistered<T>(RawMessageContext ctx) where T : class
        => (T?)ctx.RawMessage.Deserialize(_options.GetTypeInfo(typeof(T)));

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
