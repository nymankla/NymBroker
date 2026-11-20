using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using System.Diagnostics.CodeAnalysis;
using NymBroker.Core.Diagnostics;
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

    /// <summary>
    /// A thread's reusable JSON writer and buffer: every envelope sent or forwarded is written here, so a new writer,
    /// stream and growing buffer per message would be most of what serializing a small envelope allocates.
    /// </summary>
    private sealed class EnvelopeWriter
    {
        // A larger buffer (one big message) is not kept per thread.
        private const int MaxRetainedCapacity = 256 * 1024;

        [ThreadStatic] private static EnvelopeWriter? t_cached;

        private readonly ArrayBufferWriter<byte> _buffer = new(1024);

        private EnvelopeWriter() => Json = new Utf8JsonWriter(_buffer);

        public Utf8JsonWriter Json { get; }

        /// <summary>Takes the thread's writer, so a converter that serializes an envelope while this one is written gets its own.</summary>
        public static EnvelopeWriter Rent()
        {
            var writer = t_cached ?? new EnvelopeWriter();
            t_cached = null;
            return writer;
        }

        public byte[] ToArray()
        {
            Json.Flush();
            return _buffer.WrittenSpan.ToArray();
        }

        /// <summary>Clears what was written, also after a failed write, and gives the writer back to the thread.</summary>
        public void Return()
        {
            Json.Reset();
            _buffer.ResetWrittenCount();
            if (_buffer.Capacity <= MaxRetainedCapacity)
                t_cached = this;
            else
                Json.Dispose();
        }
    }

    internal byte[] WriteRegistered<TMessage>(IMessageContext context, TMessage? message)
    {
        var envelope = EnvelopeWriter.Rent();
        try
        {
            var writer = envelope.Json;
            writer.WriteStartObject();
            writer.WriteString("id", context.Id);
            writer.WriteString("correlationId", context.CorrelationId);
            WriteValue(writer, "address", context.Address, typeof(EndpointAddress));
            writer.WriteString("messageType", context.MessageType);
            writer.WriteString("created", context.Created);
            WriteTraceContext(writer, context);
            writer.WritePropertyName("message");
            JsonSerializer.Serialize<TMessage>(writer, message!, _options.GetTypeInfo<TMessage>());
            if (context.DeadLetter is not null)
                WriteValue(writer, "deadLetter", context.DeadLetter, typeof(DeadLetterInfo));
            writer.WriteEndObject();
            return envelope.ToArray();
        }
        finally
        {
            envelope.Return();
        }
    }

    /// <summary>Before <c>message</c>, so a reader meets it early; absent when the context has none (older envelopes look the same).</summary>
    private static void WriteTraceContext(Utf8JsonWriter writer, IMessageContext context)
    {
        if (context.TraceParent is null) return;
        writer.WriteString("traceParent", context.TraceParent);
        if (context.TraceState is not null)
            writer.WriteString("traceState", context.TraceState);
    }

    private void WriteValue<T>(Utf8JsonWriter writer, string name, T? value, Type type)
    {
        writer.WritePropertyName(name);
        JsonSerializer.Serialize(writer, value, _options.GetTypeInfo(type));
    }

    public IMessageContext Deserialize(string json)
        => ToRawContext(JsonSerializer.Deserialize(json, _options.GetTypeInfo<MessageContextDto<JsonElement>>()));

    public IMessageContext Deserialize(Stream stream)
        => ToRawContext(JsonSerializer.Deserialize(stream, _options.GetTypeInfo<MessageContextDto<JsonElement>>()));

    public IMessageContext Deserialize(ReadOnlySpan<byte> utf8Json)
        => ToRawContext(JsonSerializer.Deserialize(utf8Json, _options.GetTypeInfo<MessageContextDto<JsonElement>>()));

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
        => ctx.RawMessage.Deserialize(_options.GetTypeInfo<T>());

    private static RawMessageContext ToRawContext(MessageContextDto<JsonElement>? dto)
    {
        if (dto is null) throw new InvalidOperationException("Failed to deserialize message context.");

        var trace = TraceContext.Sanitize(dto.TraceParent, dto.TraceState);   // malformed or oversized context is dropped
        return new RawMessageContext
        {
            Id = dto.Id,
            CorrelationId = dto.CorrelationId,
            Address = dto.Address,
            MessageType = dto.MessageType,
            Created = dto.Created,
            DeadLetter = dto.DeadLetter,
            TraceParent = trace.TraceParent,
            TraceState = trace.TraceState,
            RawMessage = dto.Message,
            Protection = dto.Protection
        };
    }
}
