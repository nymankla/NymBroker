using System.Runtime.ExceptionServices;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Message;
using NymBroker.Core.PubSub;
using NymBroker.Core.Serialize;
using NymBroker.Core.Splitter;
using NymBroker.Core.Transform;

namespace NymBroker.Core.Impl;

internal sealed partial class NymBrokerImpl
{
    public async Task PostAsync<T>(string endpointName, T message, CancellationToken ct = default, int? splitThresholdBytes = null, bool compress = true) where T : class
    {
        var context = NewContext(message, endpointName);
        await PostToEndpointAsync(endpointName, MessageSerializerJson.SerializeToBytes(context), splitThresholdBytes, compress, ct);
    }

#pragma warning disable RS0026 // see INymBroker.PostRawAsync: the overloads differ in a required parameter
    public async Task PostRawAsync(string endpointName, Stream message, CancellationToken ct = default, int? splitThresholdBytes = null, bool compress = true)
    {
        ArgumentNullException.ThrowIfNull(message);
        // From the current position to the end, like Stream.CopyTo — works for non-seekable streams (e.g. a request body).
        using var copy = new MemoryStream();
        await message.CopyToAsync(copy, ct);
        await PostToEndpointAsync(endpointName, copy.ToArray(), splitThresholdBytes, compress, ct);
    }

    // Copied: endpoints may keep the array (the Memory endpoint queues it), so the caller's buffer must not be shared.
    public Task PostRawAsync(string endpointName, ReadOnlyMemory<byte> message, CancellationToken ct = default, int? splitThresholdBytes = null, bool compress = true)
        => PostToEndpointAsync(endpointName, message.ToArray(), splitThresholdBytes, compress, ct);
#pragma warning restore RS0026

    public async Task PostDeferredAsync<T>(string endpointName, T message, DateTimeOffset deliverAt, CancellationToken ct = default) where T : class
    {
        var context = NewContext(message, endpointName);
        // The age checks count from when the message is due: a long deferral must not expire it before its first delivery.
        if (deliverAt.UtcDateTime > context.Created) context.Created = deliverAt.UtcDateTime;

        await SendDeferredAsync(endpointName, MessageSerializerJson.SerializeToBytes(context), deliverAt, ct);
    }

#pragma warning disable RS0026 // see INymBroker.PostRawDeferredAsync: the overloads differ in a required parameter
    public async Task PostRawDeferredAsync(string endpointName, Stream message, DateTimeOffset deliverAt, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        using var copy = new MemoryStream();
        await message.CopyToAsync(copy, ct);
        await SendDeferredAsync(endpointName, copy.ToArray(), deliverAt, ct);
    }

    public Task PostRawDeferredAsync(string endpointName, ReadOnlyMemory<byte> message, DateTimeOffset deliverAt, CancellationToken ct = default)
        => SendDeferredAsync(endpointName, message.ToArray(), deliverAt, ct);
#pragma warning restore RS0026

    public async Task PublishAsync<T>(T message, CancellationToken ct = default) where T : class
    {
        var context = NewContext(message);
        var result = await ProcessAsync(MessageSerializerJson.SerializeToBytes(context), null, ct);

        // An in-process publish has no transport to redeliver it, so a Retry (e.g. a route's destination
        // failed) surfaces to the caller, as it did before ProcessAsync returned results.
        if (result.Outcome == ProcessOutcome.Retry)
        {
            if (result.Exception != null)
                ExceptionDispatchInfo.Capture(result.Exception).Throw();
            throw new InvalidOperationException(result.Description ?? "Publishing the message failed.");
        }
    }

    public async Task PublishAsync<T>(string topicName, T message, CancellationToken ct = default) where T : class
    {
        var context = NewContext(message);   // before the topic lookup, so bytes are rejected even without a topic
        await FanOutTopicAsync(ResolveTopic(topicName), message, context, source: null, ct);
    }

    public async Task PostBatchAsync<T>(string endpointName, IEnumerable<T> messages, CancellationToken ct = default,
        int? splitThresholdBytes = null, bool compress = true, Guid? correlationId = null) where T : class
    {
        var batch = ToBatchList(messages);
        var endpoint = ResolvePostTarget(endpointName);   // unknown / read-only throw before anything is sent
        if (batch.Count == 0) return;

        var wire = new List<byte[]>(batch.Count);
        foreach (var message in batch)
        {
            var context = NewContext(message, endpointName);
            if (correlationId.HasValue) context.CorrelationId = correlationId.Value;

            wire.AddRange(ToWireMessages(endpointName, MessageSerializerJson.SerializeToBytes(context), splitThresholdBytes, compress));
        }

        await SendBatchAsync(endpointName, endpoint, wire, ct);
        LogPostedBatch(batch.Count, wire.Count, endpointName);
    }

    public async Task PublishBatchAsync<T>(IEnumerable<T> messages, CancellationToken ct = default) where T : class
    {
        // Routes, topic predicates and consumers are evaluated per message, exactly as PublishAsync does.
        foreach (var message in ToBatchList(messages))
            await PublishAsync(message, ct);
    }

    public async Task PublishBatchAsync<T>(string topicName, IEnumerable<T> messages, CancellationToken ct = default) where T : class
    {
        var batch = ToBatchList(messages);
        var contexts = batch.Select(m => (IMessageContext)NewContext(m)).ToList();   // before the topic lookup, like PublishAsync
        var topic = ResolveTopic(topicName);
        if (batch.Count == 0) return;

        await DeliverToTopicEndpointsAsync(topic, contexts, MessageTypeName.Get(typeof(T)), source: null, ct);

        // Subscribers receive the messages one by one.
        if (topic.SubscriberDispatchers.Count > 0)
            for (var i = 0; i < batch.Count; i++)
                await _subscriberDispatcher.DispatchAsync(topic.SubscriberDispatchers, batch[i], contexts[i], ct);
    }

    /// <summary>
    /// Wraps a message in its envelope — the one place every Post/Publish method creates one. Bytes and streams are
    /// rejected: serialized as a message they would become a Base64 or meaningless payload the receiver drops (#74).
    /// </summary>
    private static MessageContext<T> NewContext<T>(T message, string? endpointName = null) where T : class
    {
        if (message is Stream or byte[] or ReadOnlyMemory<byte> or Memory<byte>)
            throw new ArgumentException(
                $"A {message.GetType().Name} is not a message. To post bytes as-is (an existing envelope, or raw input for an input transformer), use PostRawAsync.",
                nameof(message));

        return new MessageContext<T>
        {
            Message = message,
            Address = endpointName is null ? null : EndpointAddress.Create(endpointName)
        };
    }

    private static List<T> ToBatchList<T>(IEnumerable<T> messages) where T : class
    {
        ArgumentNullException.ThrowIfNull(messages);
        var list = messages.ToList();
        if (list.Any(static m => m is null))
            throw new ArgumentException("A batch cannot contain null messages.", nameof(messages));
        return list;
    }

    private async Task PostToEndpointAsync(string name, byte[] message, int? splitThresholdBytes, bool compress, CancellationToken ct)
    {
        var endpoint = ResolvePostTarget(name);
        foreach (var wire in ToWireMessages(name, message, splitThresholdBytes, compress))
            await SendAsync(name, endpoint, wire, ct);
    }

    // Every send to an endpoint goes through SendAsync / SendBatchAsync, so its output transformer sees all of them.
    private Task SendAsync(string name, IEndPoint endpoint, byte[] envelope, CancellationToken ct)
    {
        if (!_outputTransformers.TryGetValue(name, out var transformer)) return endpoint.PostAsync(envelope, ct);
        var output = Transform(transformer, name, envelope);
        return output == null ? Task.CompletedTask : endpoint.PostAsync(output, ct);
    }

    private async Task SendDeferredAsync(string name, byte[] envelope, DateTimeOffset deliverAt, CancellationToken ct)
    {
        var endpoint = ResolvePostTarget(name);
        // Transformed now, also for a message the broker holds, so a failure reaches the caller.
        var output = _outputTransformers.TryGetValue(name, out var transformer) ? Transform(transformer, name, envelope) : envelope;
        if (output == null) return;

        if (endpoint is IDeferredEndPoint deferred)
        {
            try
            {
                await deferred.PostDeferredAsync(output, deliverAt, ct);
                return;
            }
            catch (NotSupportedException ex)
            {
                _logger.LogDebug("Endpoint '{Name}' cannot defer this message, so the broker holds it: {Reason}", name, ex.Message);
            }
        }

        ct.ThrowIfCancellationRequested();
        if (deliverAt <= DateTimeOffset.UtcNow)
            await endpoint.PostAsync(output, ct);
        else
            HoldUntilDue(name, endpoint, output, deliverAt);
    }

    private Task SendBatchAsync(string name, IEndPoint endpoint, IReadOnlyList<byte[]> envelopes, CancellationToken ct)
    {
        if (!_outputTransformers.TryGetValue(name, out var transformer)) return endpoint.PostBatchAsync(envelopes, ct);
        var output = envelopes.Select(e => Transform(transformer, name, e)).OfType<byte[]>().ToList();
        return output.Count == 0 ? Task.CompletedTask : endpoint.PostBatchAsync(output, ct);
    }

    private byte[]? Transform(IOutputTransformer transformer, string name, byte[] envelope)
    {
        RawMessageContext context;
        try { context = (RawMessageContext)_serializer.Deserialize(envelope.AsSpan()); }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Endpoint '{name}' has an output transformer, which needs a NymBroker envelope; the bytes sent to it are not one.", ex);
        }
        return transformer.Transform(context, name);
    }

    private TopicContext ResolveTopic(string name)
    {
        foreach (var topic in _topics)
            if (string.Equals(topic.TopicName, name, StringComparison.OrdinalIgnoreCase))
                return topic;
        throw new InvalidOperationException($"No topic registered with name '{name}'.");
    }

    private IEndPoint ResolvePostTarget(string name)
    {
        if (!_endpoints.TryGetValue(name, out var endpoint))
            throw new InvalidOperationException($"No endpoint registered with name '{name}'.");
        if (endpoint.Mode == EndpointMode.ReadOnly)
            throw new InvalidOperationException($"Endpoint '{name}' is read-only and cannot receive posted messages.");
        return endpoint;
    }

    /// <summary>
    /// The envelope(s) to send for one serialized message: the message itself, or — when it exceeds
    /// <paramref name="splitThresholdBytes"/> — its (optionally compressed) <see cref="SplitMessage"/> parts, in order.
    /// </summary>
    private List<byte[]> ToWireMessages(string name, byte[] message, int? splitThresholdBytes, bool compress)
    {
        if (splitThresholdBytes is not { } threshold) return [message];
        if (_outputTransformers.ContainsKey(name))
            throw new InvalidOperationException(
                $"Endpoint '{name}' has an output transformer, so its messages cannot be split: the parts would not be in the destination's format.");
        if (message.Length <= threshold) return [message];

        var (payload, compressionName) = compress ? TryCompress(message) : (message, null);
        var parts = SplitPayload(payload, threshold, compressionName);

        _logger.LogDebug(
            "Splitting message of {Size} bytes ({PayloadSize} bytes after compression) into {Count} part(s) for endpoint '{Endpoint}' (threshold={Threshold} bytes, compression={Compression})",
            message.Length, payload.Length, parts.Count, name, threshold, compressionName ?? "none");

        return parts
            .Select(part => MessageSerializerJson.SerializeToBytes(new MessageContext<SplitMessage>
            {
                Message = part,
                Address = EndpointAddress.Create(name)
            }))
            .ToList();
    }

    /// <summary>The compressed bytes and the codec name, or the original bytes and null when compression doesn't make them smaller (e.g. already-compressed or binary data).</summary>
    private (byte[] Payload, string? CompressionName) TryCompress(byte[] message)
    {
        var compressed = _compressor.Compress(message);
        return compressed.Length < message.Length ? (compressed, _compressor.Name) : (message, null);
    }

    /// <summary>Splits the payload into <see cref="SplitMessage"/> parts of at most <paramref name="threshold"/> bytes, each tagged with the compression used.</summary>
    private List<SplitMessage> SplitPayload(byte[] payload, int threshold, string? compressionName)
    {
        var parts = _splitter.Split(payload, new DefaultSplitCondition(threshold)).ToList();
        if (parts.Count == 0)
        {
            // Compression alone brought the payload under the threshold, so the splitter saw nothing to do. It still
            // has to travel as a SplitMessage carrier (compressed bytes aren't valid JSON on their own): one part.
            parts.Add(new SplitMessage
            {
                CorrelationId = Guid.NewGuid(),
                CorrelationSequence = 0,
                GroupSize = 1,
                Body = Convert.ToBase64String(payload)
            });
        }

        if (compressionName != null)
            foreach (var part in parts) part.Compression = compressionName;
        return parts;
    }

    [LoggerMessage(LogLevel.Debug, "Posted a batch of {Count} message(s) ({WireCount} envelope(s)) to endpoint '{Endpoint}'")]
    private partial void LogPostedBatch(int count, int wireCount, string endpoint);
}
