using System.Text;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.ExceptionServices;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Message;
using NymBroker.Core.PubSub;
using NymBroker.Core.Serialize;
using NymBroker.Core.Splitter;

namespace NymBroker.Core.Impl;

public sealed partial class NymBrokerImpl
{
    public async Task PostAsync<T>(string endpointName, T message, CancellationToken ct = default, int? splitThresholdBytes = null, bool compress = true) where T : class
    {
        var context = new MessageContext<T>
        {
            Message = message,
            Address = EndpointAddress.Create(endpointName)
        };

        using var stream = _serializer.Serialize(context);
        await PostToEndpointAsync(endpointName, StreamToBytes(stream), splitThresholdBytes, compress, ct);
    }

    public async Task PostAsync(string endpointName, Stream messageStream, CancellationToken ct = default, int? splitThresholdBytes = null, bool compress = true)
        => await PostToEndpointAsync(endpointName, StreamToBytes(messageStream), splitThresholdBytes, compress, ct);

    public async Task PublishAsync<T>(T message, CancellationToken ct = default) where T : class
    {
        var context = new MessageContext<T> { Message = message };
        using var stream = _serializer.Serialize(context);
        var result = await ProcessAsync(StreamToBytes(stream), null, ct);

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
        var topic = _topics.FirstOrDefault(t => string.Equals(t.TopicName, topicName, StringComparison.OrdinalIgnoreCase));
        if (topic == null)
        {
            _logger.LogWarning("No topic registered with name '{TopicName}'", topicName);
            return;
        }

        var context = new MessageContext<T> { Message = message };
        await FanOutTopicAsync(topic, message, context, ct);
    }

    public Task<ProcessResult> ProcessAsync(string raw, string? sourceEndpoint = null, CancellationToken ct = default)
        => ProcessAsync(Encoding.UTF8.GetBytes(raw), sourceEndpoint, ct);

    public async Task<ProcessResult> ProcessAsync(byte[] raw, string? sourceEndpoint = null, CancellationToken ct = default)
    {
        if (_startInitiated && !_started) await _startGate.Task.WaitAsync(ct);

        var tags = new TagList { { "source", sourceEndpoint ?? "unknown" } };
        NymBrokerDiagnostics.MessagesReceived.Add(1, tags);
        var startedAt = Stopwatch.GetTimestamp();
        var failed = false;
        using var activity = NymBrokerActivitySource.Source.StartActivity("nymbroker.process", ActivityKind.Consumer);
        activity?.SetTag("messaging.system", "nymbroker");
        activity?.SetTag("nymbroker.source", sourceEndpoint);

        void RecordFailure(Exception? exception = null)
        {
            if (failed) return;
            failed = true;
            NymBrokerDiagnostics.MessagesFailed.Add(1, tags);
            if (exception != null)
                activity?.SetTag("error.type", exception.GetType().FullName);
            activity?.SetStatus(ActivityStatusCode.Error);
        }

        var resultTag = "cancelled";
        try
        {
            var result = await ProcessMessageAsync(raw, sourceEndpoint, ct, activity, RecordFailure);
            resultTag = ResultTag(result.Outcome);
            return result;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Unexpected failure (e.g. posting to a route's destination threw): the source endpoint
            // decides what Retry means for its transport (redeliver, or log and drop).
            RecordFailure(ex);
            _logger.LogError(ex, "Processing failed for a message from {Source}; returning Retry to the endpoint", sourceEndpoint);
            resultTag = ResultTag(ProcessOutcome.Retry);
            return ProcessResult.Retry(ex);
        }
        finally
        {
            tags.Add("outcome", failed ? "failure" : "success");
            tags.Add("result", resultTag);
            NymBrokerDiagnostics.ProcessingDuration.Record(Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds, tags);
        }
    }

    private async Task<ProcessResult> ProcessMessageAsync(
        byte[] raw,
        string? sourceEndpoint,
        CancellationToken ct,
        Activity? activity,
        Action<Exception?> recordFailure)
    {
        // Endpoints whose transport has its own dead-letter queue settle failures themselves (Retry / DeadLetter);
        // for all others the broker posts to its dead-letter endpoint and reports Completed.
        var nativeDeadLetter = UsesNativeDeadLetter(sourceEndpoint);

        // ── Wire Tap ─────────────────────────────────────────────────────────
        // Copies raw bytes to every tap endpoint before processing. Tap endpoints see
        // all messages including those that will be filtered, expired, or dead-lettered.
        foreach (var tapName in _wireTapEndpoints)
        {
            if (_endpoints.TryGetValue(tapName, out var tapEndpoint))
            {
                try { await tapEndpoint.PostAsync(raw, ct); }
                catch (Exception ex) { _logger.LogError(ex, "Wire tap failed for endpoint '{Endpoint}'", tapName); }
            }
            else
                _logger.LogWarning("Wire tap references unknown endpoint '{Endpoint}'", tapName);
        }

        // ── Deserialize ───────────────────────────────────────────────────────
        IMessageContext context;
        var transformer = sourceEndpoint != null && _endpointTransformers.TryGetValue(sourceEndpoint, out var epT)
            ? epT
            : _globalInputTransformer;

        if (transformer != null)
        {
            var transformed = transformer.Transform(raw.AsSpan(), sourceEndpoint);
            if (transformed == null) return ProcessResult.Completed;
            context = transformed;
        }
        else
        {
            try { context = _serializer.Deserialize(raw.AsSpan()); }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to deserialize message from {Source}", sourceEndpoint);
                recordFailure(ex);
                return await DeadLetterAsync(raw, sourceEndpoint, nativeDeadLetter,
                    DeadLetterReasons.DeserializationFailed, ex.Message, ex, ct);
            }
        }

        if (context.Address == null) context.Address = new EndpointAddress();
        context.Address.From = sourceEndpoint;
        activity?.SetTag("messaging.message.id", context.Id);
        activity?.SetTag("messaging.conversation_id", context.CorrelationId);
        activity?.SetTag("messaging.message.type", context.MessageType);
        using var logScope = _logger.BeginScope(new Dictionary<string, object?>
        {
            ["MessageId"] = context.Id,
            ["CorrelationId"] = context.CorrelationId,
            ["MessageType"] = context.MessageType,
            ["SourceEndpoint"] = sourceEndpoint
        });

        // ── TTL check ─────────────────────────────────────────────────────────
        // Expired messages are forwarded to the dead letter endpoint (if configured)
        // and then discarded before reaching filters, routing, or consumer dispatch.
        if (_maxMessageAge.HasValue)
        {
            var age = DateTime.UtcNow - context.Created;
            if (age > _maxMessageAge.Value)
            {
                _logger.LogWarning(
                    "Discarding expired message {MessageId} (type={MessageType}, age={Age:F1}s, ttl={Ttl:F1}s)",
                    context.Id, context.MessageType, age.TotalSeconds, _maxMessageAge.Value.TotalSeconds);
                return await DeadLetterAsync(raw, sourceEndpoint, nativeDeadLetter, DeadLetterReasons.Expired,
                    $"Message age {age.TotalSeconds:F1}s exceeds the maximum of {_maxMessageAge.Value.TotalSeconds:F1}s", null, ct);
            }
        }

        // ── Filters ───────────────────────────────────────────────────────────
        foreach (var filter in _filters)
        {
            context = filter.Filter(context)!;
            if (context == null) return ProcessResult.Completed;
        }

        if (context is not RawMessageContext raw2)
        {
            _logger.LogWarning("Unexpected context type: {Type}", context.GetType().Name);
            return ProcessResult.Completed;
        }

        // ── Resolve CLR type ──────────────────────────────────────────────────
        var messageType = _messageTypeRegistry.Resolve(raw2.MessageType);

        // ── Aggregator ────────────────────────────────────────────────────────
        if (messageType == typeof(SplitMessage))
        {
            var split = MessageSerializerJson.DeserializeMessage<SplitMessage>(raw2);
            if (split == null) return ProcessResult.Completed;

            // Earlier parts are Completed; the part that completes the group carries the reassembled message's result.
            var reassembled = await _aggregator.AddAsync(split, context, ct);
            if (reassembled == null) return ProcessResult.Completed;

            if (!string.IsNullOrEmpty(split.Compression))
            {
                if (!string.Equals(split.Compression, _compressor.Name, StringComparison.Ordinal))
                {
                    _logger.LogError("Reassembled message uses unknown compression '{Compression}' — cannot decode.", split.Compression);
                    recordFailure(null);
                    return await DeadLetterAsync(raw, sourceEndpoint, nativeDeadLetter, DeadLetterReasons.UnknownCompression,
                        $"Unknown compression '{split.Compression}'", null, ct);
                }
                reassembled = _compressor.Decompress(reassembled);
            }

            var reassembledJson = Encoding.UTF8.GetString(reassembled);
            return await ProcessAsync(reassembledJson, sourceEndpoint, ct);
        }

        var msgElement = raw2.RawMessage;

        // ── Route to destination endpoints ────────────────────────────────────
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
            var tags = new TagList
            {
                { "source", context.Address?.From ?? "unknown" },
                { "destination", route.DestinationEndpoint },
                { "message_type", raw2.MessageType ?? messageType?.FullName ?? typeof(IAnyMessage).FullName! },
                { "via", "route" }
            };
            try
            {
                await destEndpoint.PostAsync(StreamToBytes(stream), ct);
                tags.Add("outcome", "success");
                NymBrokerDiagnostics.MessagesRouted.Add(1, tags);
            }
            catch
            {
                tags.Add("outcome", "failure");
                NymBrokerDiagnostics.MessagesRouted.Add(1, tags);
                throw;
            }
            _logger.LogInformation(
                "Routed message type {MessageType} from {Source} to {Destination}",
                raw2.MessageType ?? messageType?.FullName ?? typeof(IAnyMessage).FullName,
                context.Address?.From,
                route.DestinationEndpoint);
            wasRouted = true;
        }

        // ── Topic fan-out (pub/sub) ────────────────────────────────────────────
        var wasTopicFanOut = false;
        ProcessResult? topicFailure = null;
        object? deserializedMessage = null;
        foreach (var topic in _topics)
        {
            if (!topic.Evaluate(messageType ?? typeof(IAnyMessage), context, msgElement)) continue;
            wasTopicFanOut = true;

            if (topic.SubscriberDispatchers.Count > 0 && messageType != null && deserializedMessage == null)
                deserializedMessage = MessageSerializerJson.DeserializeMessageObject(raw2, messageType);

            try
            {
                await FanOutTopicAsync(topic, deserializedMessage, context, ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                recordFailure(ex);
                if (nativeDeadLetter)
                {
                    // Let the transport redeliver; keep delivering to the remaining topics.
                    _logger.LogError(ex, "Topic '{Topic}' failed to deliver message — the endpoint will retry it", topic.TopicName);
                    topicFailure ??= ProcessResult.Retry(ex);
                }
                else
                {
                    _logger.LogError(ex, "Topic '{Topic}' failed to deliver message — routing to dead letter", topic.TopicName);
                    await TryPostToDeadLetterAsync(raw, sourceEndpoint, DeadLetterReasons.TopicDeliveryFailed, ex.Message, ex, ct);
                }
            }
        }

        if (wasRouted || wasTopicFanOut)
            return topicFailure ?? ProcessResult.Completed;

        if (messageType == null)
        {
            _logger.LogWarning("No consumer or route for unresolved message type '{MessageType}' from {Source}", raw2.MessageType, sourceEndpoint);
            return ProcessResult.Completed;
        }

        // ── Consumer dispatch — failures go to dead letter ────────────────────
        var message = MessageSerializerJson.DeserializeMessageObject(raw2, messageType);
        if (message != null)
        {
            try
            {
                await _consumerDispatcher.DispatchAsync(messageType, message, context, ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                recordFailure(ex);
                if (nativeDeadLetter)
                {
                    // The transport redelivers it and dead-letters natively once its delivery limit is reached.
                    _logger.LogError(ex, "Consumer failed for message type {MessageType} — the endpoint will retry it", messageType.Name);
                    return ProcessResult.Retry(ex);
                }

                _logger.LogError(ex, "Consumer failed for message type {MessageType} — routing to dead letter", messageType.Name);
                await TryPostToDeadLetterAsync(raw, sourceEndpoint, DeadLetterReasons.ConsumerFailed, ex.Message, ex, ct);
            }
        }

        return ProcessResult.Completed;
    }

    private bool UsesNativeDeadLetter(string? sourceEndpoint)
        => sourceEndpoint != null
           && _endpoints.TryGetValue(sourceEndpoint, out var endpoint)
           && endpoint is IEndPointEventDriven { UsesNativeDeadLetter: true };

    /// <summary>
    /// A message that can never succeed: native endpoints get <see cref="ProcessOutcome.DeadLetter"/>; otherwise it is
    /// posted to the broker's dead-letter endpoint (if configured) and reported as Completed.
    /// </summary>
    private async Task<ProcessResult> DeadLetterAsync(byte[] raw, string? sourceEndpoint, bool nativeDeadLetter,
        string reason, string? description, Exception? exception, CancellationToken ct)
    {
        if (nativeDeadLetter)
        {
            _logger.LogWarning("Message from {Source} is dead-lettered by the endpoint ({Reason}: {Description})",
                sourceEndpoint, reason, description);
            RecordDeadLettered(reason, sourceEndpoint, "native");
            return ProcessResult.DeadLetter(reason, description, exception);
        }

        await TryPostToDeadLetterAsync(raw, sourceEndpoint, reason, description, exception, ct);
        return ProcessResult.Completed;
    }

    private static void RecordDeadLettered(string reason, string? sourceEndpoint, string mode)
        => NymBrokerDiagnostics.MessagesDeadLettered.Add(1, new TagList
        {
            { "reason", reason },
            { "source", sourceEndpoint ?? "unknown" },
            { "mode", mode }
        });

    private async Task TryPostToDeadLetterAsync(byte[] raw, string? sourceEndpoint, string reason, string? description,
        Exception? exception, CancellationToken ct)
    {
        if (_deadLetterEndpoint == null) return;
        if (!_endpoints.TryGetValue(_deadLetterEndpoint, out var dlq))
        {
            _logger.LogError("Dead letter endpoint '{Endpoint}' not found", _deadLetterEndpoint);
            return;
        }
        try
        {
            var info = new DeadLetterInfo(reason, description, exception?.GetType().FullName, sourceEndpoint, DateTime.UtcNow);
            await dlq.PostAsync(DeadLetterEnvelope.Annotate(raw, info), ct);
            RecordDeadLettered(reason, sourceEndpoint, "broker");
            _logger.LogWarning("Message dead-lettered to endpoint '{Endpoint}' ({Reason}: {Description})", _deadLetterEndpoint, reason, description);
        }
        catch (Exception ex) { _logger.LogError(ex, "Failed to post message to dead letter endpoint '{Endpoint}'", _deadLetterEndpoint); }
    }

    private static string ResultTag(ProcessOutcome outcome) => outcome switch
    {
        ProcessOutcome.Completed => "completed",
        ProcessOutcome.Retry => "retry",
        _ => "dead_letter"
    };

    private async Task FanOutTopicAsync(
        TopicContext topic,
        object? message,
        IMessageContext context,
        CancellationToken ct)
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
            using var stream = _serializer.Serialize(context);
            var tags = new TagList
            {
                { "source", context.Address?.From ?? "unknown" },
                { "destination", endpointName },
                { "message_type", context.MessageType ?? (message != null ? MessageTypeName.Get(message.GetType()) : "unknown") },
                { "via", "topic" },
                { "topic", topic.TopicName }
            };
            try
            {
                await endpoint.PostAsync(StreamToBytes(stream), ct);
                tags.Add("outcome", "success");
                NymBrokerDiagnostics.MessagesRouted.Add(1, tags);
            }
            catch
            {
                tags.Add("outcome", "failure");
                NymBrokerDiagnostics.MessagesRouted.Add(1, tags);
                throw;
            }
            _logger.LogInformation("Topic '{Topic}' delivered to endpoint '{Endpoint}'", topic.TopicName, endpointName);
        }

        if (message != null && topic.SubscriberDispatchers.Count > 0)
            await _subscriberDispatcher.DispatchAsync(topic.SubscriberDispatchers, message, context, ct);
    }

    private async Task PostToEndpointAsync(string name, byte[] message, int? splitThresholdBytes, bool compress, CancellationToken ct)
    {
        if (splitThresholdBytes.HasValue && message.Length > splitThresholdBytes.Value)
        {
            var payload = message;
            string? compressionName = null;

            if (compress)
            {
                var compressed = _compressor.Compress(message);
                if (compressed.Length < message.Length)
                {
                    payload = compressed;
                    compressionName = _compressor.Name;
                }
                // else: compression didn't help (e.g. already-compressed/binary payload) — keep the original bytes.
            }

            var condition = new DefaultSplitCondition(splitThresholdBytes.Value);
            var parts = _splitter.Split(payload, condition);

            if (parts.Count == 0)
            {
                // Compression alone brought the payload under the threshold, so the splitter saw
                // nothing to do — it still has to travel as a SplitMessage carrier (compressed
                // bytes aren't valid JSON on their own), so wrap it as a single-part group.
                parts =
                [
                    new SplitMessage
                    {
                        CorrelationId = Guid.NewGuid(),
                        CorrelationSequence = 0,
                        GroupSize = 1,
                        Body = Convert.ToBase64String(payload)
                    }
                ];
            }

            if (compressionName != null)
                foreach (var part in parts) part.Compression = compressionName;

            _logger.LogInformation(
                "Splitting message of {Size} bytes ({PayloadSize} bytes after compression) into {Count} part(s) for endpoint '{Endpoint}' (threshold={Threshold} bytes, compression={Compression})",
                message.Length, payload.Length, parts.Count, name, splitThresholdBytes.Value, compressionName ?? "none");

            foreach (var part in parts)
            {
                var partContext = new MessageContext<SplitMessage>
                {
                    Message = part,
                    Address = EndpointAddress.Create(name)
                };
                using var partStream = _serializer.Serialize(partContext);
                await PostToEndpointAsync(name, StreamToBytes(partStream), null, false, ct);
            }
            return;
        }

        await PostToEndpointAsync(name, message, ct);
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
