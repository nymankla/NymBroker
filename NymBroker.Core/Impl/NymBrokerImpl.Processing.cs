using System.Text;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Message;
using NymBroker.Core.PubSub;
using NymBroker.Core.Serialize;

namespace NymBroker.Core.Impl;

public sealed partial class NymBrokerImpl
{
    public async Task PostAsync<T>(string endpointName, T message, CancellationToken ct = default) where T : class
    {
        var context = new MessageContext<T>
        {
            Message = message,
            Address = EndpointAddress.Create(endpointName)
        };

        using var stream = _serializer.Serialize(context);
        await PostToEndpointAsync(endpointName, StreamToBytes(stream), ct);
    }

    public async Task PostAsync(string endpointName, Stream messageStream, CancellationToken ct = default)
        => await PostToEndpointAsync(endpointName, StreamToBytes(messageStream), ct);

    public Task PublishAsync<T>(T message, CancellationToken ct = default) where T : class
    {
        var context = new MessageContext<T> { Message = message };
        using var stream = _serializer.Serialize(context);
        var bytes = StreamToBytes(stream);
        return ProcessAsync(bytes, null, ct);
    }

    public async Task PublishAsync<T>(string topicName, T message, CancellationToken ct = default) where T : class
    {
        var topic = _topics.FirstOrDefault(t => string.Equals(t.TopicName, topicName, StringComparison.OrdinalIgnoreCase));
        if (topic == null)
        {
            _logger.LogWarning("No topic registered with name '{TopicName}'", topicName);
            return;
        }

        var context = new MessageContext<T> { Message = message };
        await FanOutTopicAsync(topic, message, context, ct);
    }

    public Task ProcessAsync(string raw, string? sourceEndpoint = null, CancellationToken ct = default)
        => ProcessAsync(Encoding.UTF8.GetBytes(raw), sourceEndpoint, ct);

    public async Task ProcessAsync(byte[] raw, string? sourceEndpoint = null, CancellationToken ct = default)
    {
        if (_startInitiated && !_started) await _startGate.Task.WaitAsync(ct);

        IMessageContext context;
        var transformer = sourceEndpoint != null && _endpointTransformers.TryGetValue(sourceEndpoint, out var epT)
            ? epT
            : _globalInputTransformer;

        if (transformer != null)
        {
            var transformed = transformer.Transform(raw.AsSpan(), sourceEndpoint);
            if (transformed == null) return;
            context = transformed;
        }
        else
        {
            try { context = _serializer.Deserialize(raw.AsSpan()); }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to deserialize message from {Source}", sourceEndpoint);
                return;
            }
        }

        if (context.Address == null) context.Address = new EndpointAddress();
        context.Address.From = sourceEndpoint;

        // Apply filters.
        foreach (var filter in _filters)
        {
            context = filter.Filter(context)!;
            if (context == null) return;
        }

        if (context is not RawMessageContext raw2)
        {
            _logger.LogWarning("Unexpected context type: {Type}", context.GetType().Name);
            return;
        }

        // Resolve CLR type.
        var messageType = _messageTypeRegistry.Resolve(raw2.MessageType);

        // Aggregator: collect SplitMessage parts before further processing.
        if (messageType == typeof(SplitMessage))
        {
            var split = MessageSerializerJson.DeserializeMessage<SplitMessage>(raw2);
            if (split == null) return;

            var reassembled = await _aggregator.AddAsync(split, context, ct);
            if (reassembled == null) return;

            var reassembledJson = Encoding.UTF8.GetString(reassembled);
            await ProcessAsync(reassembledJson, sourceEndpoint, ct);
            return;
        }

        var msgElement = raw2.RawMessage;

        // Route to destination endpoints.
        var wasRouted = false;
        foreach (var route in _routes)
        {
            if (!route.Evaluate(messageType ?? typeof(IAnyMessage), context, msgElement)) continue;

            if (!_endpoints.TryGetValue(route.DestinationEndpoint, out var destEndpoint))
            {
                _logger.LogWarning("Route references unknown endpoint '{Dest}'", route.DestinationEndpoint);
                continue;
            }

            using var stream = _serializer.Serialize(context);
            await destEndpoint.PostAsync(StreamToBytes(stream), ct);
            _logger.LogInformation(
                "Routed message type {MessageType} from {Source} to {Destination}",
                raw2.MessageType ?? messageType?.FullName ?? typeof(IAnyMessage).FullName,
                context.Address?.From,
                route.DestinationEndpoint);
            wasRouted = true;
        }

        // Topic fan-out (pub/sub) — evaluated regardless of whether a route also matched.
        var wasTopicFanOut = false;
        object? deserializedMessage = null;
        foreach (var topic in _topics)
        {
            if (!topic.Evaluate(messageType ?? typeof(IAnyMessage), context, msgElement)) continue;
            wasTopicFanOut = true;

            // Lazily deserialize once — reused across multiple matching topics.
            if (topic.SubscriberDispatchers.Count > 0 && messageType != null && deserializedMessage == null)
                deserializedMessage = MessageSerializerJson.DeserializeMessageObject(raw2, messageType);

            await FanOutTopicAsync(topic, deserializedMessage, context, ct);
        }

        if (wasRouted || wasTopicFanOut)
            return;

        if (messageType == null)
        {
            _logger.LogWarning("No consumer or route for unresolved message type '{MessageType}' from {Source}", raw2.MessageType, sourceEndpoint);
            return;
        }

        var message = MessageSerializerJson.DeserializeMessageObject(raw2, messageType);
        if (message != null)
            await _consumerDispatcher.DispatchAsync(messageType, message, context, ct);
    }

    private async Task FanOutTopicAsync(TopicContext topic, object? message, IMessageContext context, CancellationToken ct)
    {
        foreach (var endpointName in topic.SubscriberEndpoints)
        {
            if (!_endpoints.TryGetValue(endpointName, out var endpoint))
            {
                _logger.LogWarning("Topic '{Topic}' references unknown endpoint '{Endpoint}'", topic.TopicName, endpointName);
                continue;
            }
            if (endpoint.Mode == EndpointMode.ReadOnly)
            {
                _logger.LogWarning("Topic '{Topic}' cannot deliver to read-only endpoint '{Endpoint}'", topic.TopicName, endpointName);
                continue;
            }
            try
            {
                using var stream = _serializer.Serialize(context);
                await endpoint.PostAsync(StreamToBytes(stream), ct);
                _logger.LogInformation("Topic '{Topic}' delivered to endpoint '{Endpoint}'", topic.TopicName, endpointName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Topic '{Topic}' failed to deliver to endpoint '{Endpoint}'", topic.TopicName, endpointName);
            }
        }

        if (message != null && topic.SubscriberDispatchers.Count > 0)
            await _subscriberDispatcher.DispatchAsync(topic.SubscriberDispatchers, message, context, ct);
    }

    private async Task PostToEndpointAsync(string name, byte[] message, CancellationToken ct)
    {
        if (!_endpoints.TryGetValue(name, out var endpoint))
            throw new InvalidOperationException($"No endpoint registered with name '{name}'.");
        if (endpoint.Mode == EndpointMode.ReadOnly)
            throw new InvalidOperationException($"Endpoint '{name}' is read-only and cannot receive posted messages.");
        await endpoint.PostAsync(message, ct);
    }

    private static byte[] StreamToBytes(Stream stream)
    {
        if (stream is MemoryStream ms && ms.TryGetBuffer(out var buf))
        {
            var bytes = new byte[buf.Count];
            buf.Array.AsSpan(buf.Offset, buf.Count).CopyTo(bytes);
            return bytes;
        }
        stream.Position = 0;
        using var copy = new MemoryStream();
        stream.CopyTo(copy);
        return copy.ToArray();
    }
}
