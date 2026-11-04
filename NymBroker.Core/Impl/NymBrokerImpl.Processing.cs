using System.Collections;
using System.Text;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Diagnostics;
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
        await FanOutTopicAsync(ResolveTopic(topicName), message, context, ct);
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

        await DeliverToTopicEndpointsAsync(topic, contexts, MessageTypeName.Get(typeof(T)), ct);

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

    public Task<ProcessResult> ProcessAsync(string raw, string? sourceEndpoint = null, CancellationToken ct = default)
        => ProcessAsync(Encoding.UTF8.GetBytes(raw), sourceEndpoint, ct);

    public async Task<ProcessResult> ProcessAsync(byte[] raw, string? sourceEndpoint = null, CancellationToken ct = default)
    {
        if (_startInitiated && !_started) await _startGate.Task.WaitAsync(ct);

        var tags = new TagList { { "source", sourceEndpoint ?? "unknown" } };
        NymBrokerDiagnostics.MessagesReceived.Add(1, tags);
        var startedAt = Stopwatch.GetTimestamp();
        using var activity = NymBrokerActivitySource.Source.StartActivity("nymbroker.process", ActivityKind.Consumer);
        activity?.SetTag("messaging.system", "nymbroker");
        activity?.SetTag("nymbroker.source", sourceEndpoint);
        var run = new ProcessingRun(sourceEndpoint, UsesNativeDeadLetter(sourceEndpoint), activity, tags);

        var resultTag = "cancelled";
        try
        {
            var result = await ProcessMessageAsync(raw, run, ct);
            resultTag = ResultTag(result.Outcome);
            return result;
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            // Unexpected failure (e.g. posting to a route's destination threw): the source endpoint
            // decides what Retry means for its transport (redeliver, or log and drop).
            run.RecordFailure(ex);
            _logger.LogError(ex, "Processing failed for a message from {Source}; returning Retry to the endpoint", sourceEndpoint);
            resultTag = ResultTag(ProcessOutcome.Retry);
            return ProcessResult.Retry(ex);
        }
        finally
        {
            tags.Add("outcome", run.Failed ? "failure" : "success");
            tags.Add("result", resultTag);
            NymBrokerDiagnostics.ProcessingDuration.Record(Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds, tags);
        }
    }

    /// <summary>One received message on its way through the pipeline; a split message's parts and whole share one.</summary>
    private sealed class ProcessingRun(string? source, bool nativeDeadLetter, Activity? activity, TagList tags)
    {
        private readonly TagList _tags = tags;

        public string? Source { get; } = source;

        /// <summary>
        /// The transport has its own dead-letter queue and settles failures itself (Retry / DeadLetter); for all others
        /// the broker posts to its dead-letter endpoint and reports Completed.
        /// </summary>
        public bool NativeDeadLetter { get; } = nativeDeadLetter;
        public Activity? Activity { get; } = activity;
        public bool Failed { get; private set; }

        public void RecordFailure(Exception? exception = null)
        {
            if (Failed) return;
            Failed = true;
            NymBrokerDiagnostics.MessagesFailed.Add(1, in _tags);
            if (exception != null)
                Activity?.SetTag("error.type", exception.GetType().FullName);
            Activity?.SetStatus(ActivityStatusCode.Error);
        }
    }

    private async Task<ProcessResult> ProcessMessageAsync(byte[] raw, ProcessingRun run, CancellationToken ct)
    {
        var sourceEndpoint = run.Source;

        // ── Wire Tap ─────────────────────────────────────────────────────────
        // Copies raw bytes to every tap endpoint before processing. Tap endpoints see
        // all messages including those that will be filtered, expired, or dead-lettered.
        foreach (var tapName in _wireTapEndpoints)
        {
            if (_endpoints.TryGetValue(tapName, out var tapEndpoint))
            {
                try { await SendAsync(tapName, tapEndpoint, raw, ct); }
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
                run.RecordFailure(ex);
                return await DeadLetterAsync(raw, run, DeadLetterReasons.DeserializationFailed, ex.Message, ex, ct);
            }
        }

        context.Address ??= new EndpointAddress();
        context.Address.From = sourceEndpoint;
        run.Activity?.SetTag("messaging.message.id", context.Id);
        run.Activity?.SetTag("messaging.conversation_id", context.CorrelationId);
        run.Activity?.SetTag("messaging.message.type", context.MessageType);
        using var logScope = _logger.BeginScope(new MessageLogScope(context, sourceEndpoint));

        var age = DateTime.UtcNow - context.Created;
        // A transformer may leave Created unset; negative ages are clock skew between hosts.
        if (context.Created != default)
            NymBrokerDiagnostics.MessageAge.Record(Math.Max(0, age.TotalSeconds),
                new KeyValuePair<string, object?>("source", sourceEndpoint));

        // ── TTL check ─────────────────────────────────────────────────────────
        // Expired messages are forwarded to the dead letter endpoint (if configured)
        // and then discarded before reaching filters, routing, or consumer dispatch.
        if (age > _maxMessageAge)
        {
            var maxAge = _maxMessageAge.Value;
            _logger.LogWarning(
                "Discarding expired message {MessageId} (type={MessageType}, age={Age:F1}s, ttl={Ttl:F1}s)",
                context.Id, context.MessageType, age.TotalSeconds, maxAge.TotalSeconds);
            return await DeadLetterAsync(raw, run, DeadLetterReasons.Expired,
                $"Message age {age.TotalSeconds:F1}s exceeds the maximum of {maxAge.TotalSeconds:F1}s", null, ct);
        }

        // ── Filters ───────────────────────────────────────────────────────────
        foreach (var filter in _filters)
        {
            context = filter.Filter(context)!;
            if (context == null) return ProcessResult.Completed;
        }

        if (context is not RawMessageContext rawContext)
        {
            _logger.LogWarning("Unexpected context type: {Type}", context.GetType().Name);
            return ProcessResult.Completed;
        }

        // ── Resolve CLR type ──────────────────────────────────────────────────
        var messageType = _messageTypeRegistry.Resolve(rawContext.MessageType);

        // ── Aggregator ────────────────────────────────────────────────────────
        if (messageType == typeof(SplitMessage))
        {
            var split = MessageSerializerJson.DeserializeMessage<SplitMessage>(rawContext);
            return split == null ? ProcessResult.Completed : await ProcessSplitPartAsync(raw, split, context, run, ct);
        }

        // ── Idempotent receiver ───────────────────────────────────────────────
        // Claimed after reassembly, so a split message is claimed once by its own ID, not per part.
        return _idempotencyStore is { } store
            ? await ProcessIdempotentlyAsync(store, raw, rawContext, messageType, run, ct)
            : await RouteAndDispatchAsync(raw, rawContext, messageType, run, ct);
    }

    private async Task<ProcessResult> ProcessSplitPartAsync(byte[] raw, SplitMessage split, IMessageContext context, ProcessingRun run, CancellationToken ct)
    {
        // Earlier parts are Completed; the part that completes the group carries the reassembled message's result.
        var reassembled = await _aggregator.AddAsync(split, context, ct);
        if (reassembled == null) return ProcessResult.Completed;

        var decoded = reassembled;
        if (!string.IsNullOrEmpty(split.Compression))
        {
            if (!string.Equals(split.Compression, _compressor.Name, StringComparison.Ordinal))
            {
                _logger.LogError("Reassembled message uses unknown compression '{Compression}' — cannot decode.", split.Compression);
                run.RecordFailure();
                return await DeadLetterAsync(raw, run, DeadLetterReasons.UnknownCompression, $"Unknown compression '{split.Compression}'", null, ct);
            }
            decoded = _compressor.Decompress(reassembled);
        }

        // Same run: one received message, one processing duration.
        ProcessResult result;
        try
        {
            result = await ProcessMessageAsync(decoded, run, ct);
        }
        catch
        {
            _aggregator.KeepForRetry(split.CorrelationId, reassembled);
            throw;
        }

        if (result.Outcome == ProcessOutcome.Retry)
            _aggregator.KeepForRetry(split.CorrelationId, reassembled);
        return result;
    }

    /// <summary>Routes, fans out to topics and dispatches to the consumer — the part of processing an idempotency claim covers.</summary>
    private async Task<ProcessResult> RouteAndDispatchAsync(byte[] raw, RawMessageContext context, Type? messageType, ProcessingRun run, CancellationToken ct)
    {
        var sourceEndpoint = run.Source;
        var msgElement = context.RawMessage;
        var messageTypeName = context.MessageType ?? messageType?.FullName ?? typeof(IAnyMessage).FullName!;

        // ── Route to destination endpoints ────────────────────────────────────
        var wasRouted = false;
        byte[]? routedEnvelope = null;   // identical for every route, so serialized at most once
        foreach (var route in _routes)
        {
            if (!route.Evaluate(messageType ?? typeof(IAnyMessage), context, msgElement)) continue;

            if (!_endpoints.TryGetValue(route.DestinationEndpoint, out var destEndpoint))
            {
                _logger.LogWarning("Route references unknown endpoint '{Dest}'", route.DestinationEndpoint);
                continue;
            }

            routedEnvelope ??= MessageSerializerJson.SerializeToBytes(context);
            var tags = new TagList
            {
                { "source", context.Address?.From ?? "unknown" },
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
            LogRouted(messageTypeName, context.Address?.From, route.DestinationEndpoint);
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
                deserializedMessage = MessageSerializerJson.DeserializeMessageObject(context, messageType);

            try
            {
                await FanOutTopicAsync(topic, deserializedMessage, context, ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
            {
                run.RecordFailure(ex);
                if (run.NativeDeadLetter)
                {
                    // Let the transport redeliver; keep delivering to the remaining topics.
                    _logger.LogError(ex, "Topic '{Topic}' failed to deliver message — the endpoint will retry it", topic.TopicName);
                    topicFailure ??= ProcessResult.Retry(ex);
                }
                else
                {
                    _logger.LogError(ex, "Topic '{Topic}' failed to deliver message — routing to dead letter", topic.TopicName);
                    if (await TryPostToDeadLetterAsync(raw, sourceEndpoint, DeadLetterReasons.TopicDeliveryFailed, ex.Message, ex, ct) is { } deadLetterFailure)
                        topicFailure ??= deadLetterFailure;
                }
            }
        }

        if (wasRouted || wasTopicFanOut)
            return topicFailure ?? ProcessResult.Completed;

        if (messageType == null)
        {
            _logger.LogWarning("No consumer or route for unresolved message type '{MessageType}' from {Source}", context.MessageType, sourceEndpoint);
            return ProcessResult.Completed;
        }

        // ── Consumer dispatch — failures go to dead letter ────────────────────
        var message = MessageSerializerJson.DeserializeMessageObject(context, messageType);
        if (message != null)
        {
            try
            {
                await _consumerDispatcher.DispatchAsync(messageType, message, context, ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
            {
                run.RecordFailure(ex);
                if (run.NativeDeadLetter && _noRetryMessageTypes.Contains(messageType))
                {
                    _logger.LogError(ex, "Consumer failed for message type {MessageType} — retry is off for its consumer, so the endpoint dead-letters it", messageType.Name);
                    RecordDeadLettered(DeadLetterReasons.ConsumerFailed, sourceEndpoint, "native");
                    return ProcessResult.DeadLetter(DeadLetterReasons.ConsumerFailed, ex.Message, ex);
                }

                if (run.NativeDeadLetter)
                {
                    // The transport redelivers it and dead-letters natively once its delivery limit is reached.
                    _logger.LogError(ex, "Consumer failed for message type {MessageType} — the endpoint will retry it", messageType.Name);
                    return ProcessResult.Retry(ex);
                }

                _logger.LogError(ex, "Consumer failed for message type {MessageType} — routing to dead letter", messageType.Name);
                return await TryPostToDeadLetterAsync(raw, sourceEndpoint, DeadLetterReasons.ConsumerFailed, ex.Message, ex, ct) ?? ProcessResult.Completed;
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
    /// posted to the broker's dead-letter endpoint (if configured) and reported as Completed, or Retry if that post failed.
    /// </summary>
    private async Task<ProcessResult> DeadLetterAsync(byte[] raw, ProcessingRun run, string reason, string? description, Exception? exception, CancellationToken ct)
    {
        if (run.NativeDeadLetter)
        {
            _logger.LogWarning("Message from {Source} is dead-lettered by the endpoint ({Reason}: {Description})",
                run.Source, reason, description);
            RecordDeadLettered(reason, run.Source, "native");
            return ProcessResult.DeadLetter(reason, description, exception);
        }

        return await TryPostToDeadLetterAsync(raw, run.Source, reason, description, exception, ct) ?? ProcessResult.Completed;
    }

    private static void RecordDeadLettered(string reason, string? sourceEndpoint, string mode)
        => NymBrokerDiagnostics.MessagesDeadLettered.Add(1, new TagList
        {
            { "reason", reason },
            { "source", sourceEndpoint ?? "unknown" },
            { "mode", mode }
        });

    /// <summary>Returns null if the message was posted or no dead-letter endpoint is configured, otherwise the Retry that keeps it.</summary>
    private async Task<ProcessResult?> TryPostToDeadLetterAsync(byte[] raw, string? sourceEndpoint, string reason, string? description,
        Exception? exception, CancellationToken ct)
    {
        if (_deadLetterEndpoint == null) return null;
        if (!_endpoints.TryGetValue(_deadLetterEndpoint, out var dlq))
        {
            _logger.LogError("Dead letter endpoint '{Endpoint}' not found", _deadLetterEndpoint);
            return ProcessResult.Retry(description: $"Dead letter endpoint '{_deadLetterEndpoint}' not found.");
        }
        try
        {
            var info = new DeadLetterInfo(reason, description, exception?.GetType().FullName, sourceEndpoint, DateTime.UtcNow);
            await SendAsync(_deadLetterEndpoint, dlq, DeadLetterEnvelope.Annotate(raw, info), ct);
            RecordDeadLettered(reason, sourceEndpoint, "broker");
            _logger.LogWarning("Message dead-lettered to endpoint '{Endpoint}' ({Reason}: {Description})", _deadLetterEndpoint, reason, description);
            return null;
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            _logger.LogError(ex, "Failed to post message to dead letter endpoint '{Endpoint}'; returning Retry to the source endpoint", _deadLetterEndpoint);
            return ProcessResult.Retry(ex);
        }
    }

    private static void RecordRouted(TagList tags, int count, string outcome)
    {
        tags.Add("outcome", outcome);
        NymBrokerDiagnostics.MessagesRouted.Add(count, tags);
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
        await DeliverToTopicEndpointsAsync(topic, [context],
            context.MessageType ?? (message != null ? MessageTypeName.Get(message.GetType()) : "unknown"), ct);

        if (message != null && topic.SubscriberDispatchers.Count > 0)
            await _subscriberDispatcher.DispatchAsync(topic.SubscriberDispatchers, message, context, ct);
    }

    /// <summary>
    /// Sends a copy of each message to every subscriber endpoint of the topic: one <c>PostAsync</c> for a single message,
    /// one <see cref="IEndPoint.PostBatchAsync"/> per endpoint for several. The routed counter is recorded per message.
    /// </summary>
    private async Task DeliverToTopicEndpointsAsync(TopicContext topic, IReadOnlyList<IMessageContext> contexts,
        string messageType, CancellationToken ct)
    {
        List<byte[]>? wire = null;   // the same envelopes go to every endpoint, so serialized at most once
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

            if (wire == null)
            {
                wire = new List<byte[]>(contexts.Count);
                foreach (var context in contexts)
                    wire.Add(MessageSerializerJson.SerializeToBytes(context));
            }

            var tags = new TagList
            {
                { "source", contexts[0].Address?.From ?? "unknown" },
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
        if (splitThresholdBytes.HasValue && _outputTransformers.ContainsKey(name))
            throw new InvalidOperationException(
                $"Endpoint '{name}' has an output transformer, so its messages cannot be split: the parts would not be in the destination's format.");
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

            _logger.LogDebug(
                "Splitting message of {Size} bytes ({PayloadSize} bytes after compression) into {Count} part(s) for endpoint '{Endpoint}' (threshold={Threshold} bytes, compression={Compression})",
                message.Length, payload.Length, parts.Count, name, splitThresholdBytes.Value, compressionName ?? "none");

            var wire = new List<byte[]>(parts.Count);
            foreach (var part in parts)
            {
                var partContext = new MessageContext<SplitMessage>
                {
                    Message = part,
                    Address = EndpointAddress.Create(name)
                };
                wire.Add(MessageSerializerJson.SerializeToBytes(partContext));
            }
            return wire;
        }

        return [message];
    }

    [LoggerMessage(LogLevel.Debug, "Posted a batch of {Count} message(s) ({WireCount} envelope(s)) to endpoint '{Endpoint}'")]
    private partial void LogPostedBatch(int count, int wireCount, string endpoint);

    [LoggerMessage(LogLevel.Debug, "Routed message type {MessageType} from {Source} to {Destination}")]
    private partial void LogRouted(string messageType, string? source, string destination);

    [LoggerMessage(LogLevel.Debug, "Topic '{Topic}' delivered {Count} message(s) to endpoint '{Endpoint}'")]
    private partial void LogTopicDelivered(string topic, int count, string endpoint);

    /// <summary>The log scope of one message: allocates one small object, and a boxed id only if a logger reads the scope.</summary>
    private sealed class MessageLogScope(IMessageContext context, string? sourceEndpoint) : IReadOnlyList<KeyValuePair<string, object?>>
    {
        public int Count => 4;

        public KeyValuePair<string, object?> this[int index] => index switch
        {
            0 => new("MessageId", context.Id),
            1 => new("CorrelationId", context.CorrelationId),
            2 => new("MessageType", context.MessageType),
            3 => new("SourceEndpoint", sourceEndpoint),
            _ => throw new ArgumentOutOfRangeException(nameof(index))
        };

        public IEnumerator<KeyValuePair<string, object?>> GetEnumerator()
        {
            for (var i = 0; i < Count; i++)
                yield return this[i];
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public override string ToString() => $"MessageId:{context.Id} CorrelationId:{context.CorrelationId} MessageType:{context.MessageType} SourceEndpoint:{sourceEndpoint}";
    }
}
