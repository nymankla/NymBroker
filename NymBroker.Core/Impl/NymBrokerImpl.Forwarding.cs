using System.Diagnostics;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Message;
using NymBroker.Core.PubSub;
using NymBroker.Core.Serialize;

namespace NymBroker.Core.Impl;

internal sealed partial class NymBrokerImpl
{
    /// <summary>Sends the envelope to every matching route's destination; a failed send throws (the caller returns Retry).</summary>
    private async ValueTask<bool> RouteToDestinationsAsync(RawMessageContext context, Type? messageType, string? source, CancellationToken ct)
    {
        var msgElement = context.RawMessage;
        var messageTypeName = context.MessageType ?? messageType?.FullName ?? typeof(IAnyMessage).FullName!;

        var wasRouted = false;
        foreach (var route in _routes)
        {
            if (!route.Evaluate(messageType ?? typeof(IAnyMessage), context, msgElement)) continue;

            if (!_endpoints.TryGetValue(route.DestinationEndpoint, out var destEndpoint))
            {
                _logger.LogWarning("Route references unknown endpoint '{Dest}'", route.DestinationEndpoint);
                continue;
            }

            var routedEnvelope = SerializeForwarded(context, route.DestinationEndpoint, source);
            var tags = new TagList
            {
                { "source", source ?? "unknown" },
                { "destination", route.DestinationEndpoint },
                { "message_type", messageTypeName },
                { "via", "route" }
            };
            try
            {
                await SendAsync(route.DestinationEndpoint, destEndpoint, routedEnvelope, ct);
                RecordRouted(tags, 1, "success");
            }
            catch
            {
                RecordRouted(tags, 1, "failure");
                throw;
            }
            LogRouted(messageTypeName, source, route.DestinationEndpoint);
            wasRouted = true;
        }

        return wasRouted;
    }

    /// <summary>
    /// Pub/sub: delivers to every matching topic. A failed topic is settled and the remaining topics are still
    /// delivered to; <c>Failure</c> is the first failure's result (null if all succeeded or were dead-lettered).
    /// </summary>
    private async ValueTask<(bool FannedOut, ProcessResult? Failure)> FanOutToTopicsAsync(
        byte[] raw, RawMessageContext context, Type? messageType, ProcessingRun run, CancellationToken ct)
    {
        var msgElement = context.RawMessage;
        var fannedOut = false;
        ProcessResult? failure = null;
        object? deserializedMessage = null;
        foreach (var topic in _topics)
        {
            if (!topic.Evaluate(messageType ?? typeof(IAnyMessage), context, msgElement)) continue;
            fannedOut = true;

            if (topic.SubscriberDispatchers.Count > 0 && messageType != null && deserializedMessage == null)
                deserializedMessage = MessageSerializerJson.DeserializeMessageObject(context, messageType);

            try
            {
                await FanOutTopicAsync(topic, deserializedMessage, context, run.Source, ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
            {
                // Settled for every failed topic (each posts its own dead letter); only the first result is kept.
                var result = await SettleTopicFailureAsync(topic, raw, run, ex, ct);
                failure ??= result;
            }
        }

        return (fannedOut, failure);
    }

    /// <summary>A topic failed to deliver: log it and settle it; the caller keeps delivering to the remaining topics.</summary>
    private async Task<ProcessResult?> SettleTopicFailureAsync(TopicContext topic, byte[] raw, ProcessingRun run, Exception ex, CancellationToken ct)
    {
        run.RecordFailure(ex);
        var settlement = SettlementFor(run, retryOff: false);
        if (settlement == FailureSettlement.EndpointRetries)
            _logger.LogError(ex, "Topic '{Topic}' failed to deliver message — the endpoint will retry it", topic.TopicName);
        else
            _logger.LogError(ex, "Topic '{Topic}' failed to deliver message — routing to dead letter", topic.TopicName);

        return await SettleFailureAsync(settlement, raw, run, DeadLetterCause.Of(DeadLetterReasons.TopicDeliveryFailed, ex), ct);
    }

    private static void RecordRouted(TagList tags, int count, string outcome)
    {
        tags.Add("outcome", outcome);
        NymBrokerDiagnostics.MessagesRouted.Add(count, tags);
    }

    /// <param name="source">The endpoint the message was received on; null for a publish.</param>
    private async Task FanOutTopicAsync(
        TopicContext topic,
        object? message,
        IMessageContext context,
        string? source,
        CancellationToken ct)
    {
        await DeliverToTopicEndpointsAsync(topic, [context],
            context.MessageType ?? (message != null ? MessageTypeName.Get(message.GetType()) : "unknown"), source, ct);

        if (message != null && topic.SubscriberDispatchers.Count > 0)
            await _subscriberDispatcher.DispatchAsync(topic.SubscriberDispatchers, message, context, ct);
    }

    /// <summary>
    /// Sends a copy of each message to every subscriber endpoint of the topic: one <c>PostAsync</c> for a single message,
    /// one <see cref="IEndPoint.PostBatchAsync"/> per endpoint for several. The routed counter is recorded per message.
    /// </summary>
    private async Task DeliverToTopicEndpointsAsync(TopicContext topic, IReadOnlyList<IMessageContext> contexts,
        string messageType, string? source, CancellationToken ct)
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

            var wire = new List<byte[]>(contexts.Count);
            foreach (var context in contexts)
                wire.Add(SerializeForwarded(context, endpointName, source));

            var tags = new TagList
            {
                { "source", source ?? "unknown" },
                { "destination", endpointName },
                { "message_type", messageType },
                { "via", "topic" },
                { "topic", topic.TopicName }
            };
            try
            {
                if (wire.Count == 1)
                    await SendAsync(endpointName, endpoint, wire[0], ct);
                else
                    await SendBatchAsync(endpointName, endpoint, wire, ct);
                RecordRouted(tags, wire.Count, "success");
            }
            catch
            {
                RecordRouted(tags, wire.Count, "failure");
                throw;
            }
            LogTopicDelivered(topic.TopicName, wire.Count, endpointName);
        }
    }

    /// <summary>
    /// Serializes the envelope for one forwarding destination (a route or a topic's subscriber endpoint): <c>address.to</c>
    /// is that destination and <c>address.from</c> the endpoint the broker received the message on (null for a publish),
    /// so an envelope in a queue says where it is and where it came from. The context's own address is left unchanged
    /// for the subscribers and consumers that see it afterwards.
    /// </summary>
    private static byte[] SerializeForwarded(IMessageContext context, string destination, string? source)
    {
        var original = context.Address;
        context.Address = EndpointAddress.Create(destination, source);
        try
        {
            return MessageSerializerJson.SerializeToBytes(context);
        }
        finally
        {
            context.Address = original;
        }
    }

    [LoggerMessage(LogLevel.Debug, "Routed message type {MessageType} from {Source} to {Destination}")]
    private partial void LogRouted(string messageType, string? source, string destination);

    [LoggerMessage(LogLevel.Debug, "Topic '{Topic}' delivered {Count} message(s) to endpoint '{Endpoint}'")]
    private partial void LogTopicDelivered(string topic, int count, string endpoint);
}
