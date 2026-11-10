using System.Collections;
using System.Text;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;

namespace NymBroker.Core.Impl;

internal sealed partial class NymBrokerImpl
{
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
        await WireTapAsync(raw, ct);

        var decoded = Decode(raw, run, out var decodeError);
        if (decoded == null)
            return decodeError == null
                ? ProcessResult.Completed   // the input transformer dropped it
                : await DeadLetterAsync(raw, run, DeadLetterCause.Of(DeadLetterReasons.DeserializationFailed, decodeError), ct);

        StampReceived(decoded, run);
        using var logScope = _logger.BeginScope(new MessageLogScope(decoded, run.Source));

        // Expired messages are forwarded to the dead letter endpoint (if configured)
        // and then discarded before reaching filters, routing, or consumer dispatch.
        if (CheckExpired(decoded, run.Source) is { } expired)
            return await DeadLetterAsync(raw, run, new DeadLetterCause(DeadLetterReasons.Expired, expired, null), ct);

        var context = ApplyFilters(decoded);
        if (context == null) return ProcessResult.Completed;

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
        return _idempotencyStore is not null
            ? await ProcessIdempotentlyAsync(raw, rawContext, messageType, run, ct)
            : await RouteAndDispatchAsync(raw, rawContext, messageType, run, ct);
    }

    /// <summary>
    /// Copies raw bytes to every tap endpoint before processing. Tap endpoints see
    /// all messages including those that will be filtered, expired, or dead-lettered.
    /// </summary>
    private async Task WireTapAsync(byte[] raw, CancellationToken ct)
    {
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
    }

    /// <summary>
    /// Decodes the raw bytes with the source's input transformer, the global one, or the envelope serializer.
    /// Returns null when a transformer drops the message (<paramref name="error"/> null) or the envelope
    /// cannot be deserialized (<paramref name="error"/> set, failure logged and recorded).
    /// </summary>
    private IMessageContext? Decode(byte[] raw, ProcessingRun run, out Exception? error)
    {
        error = null;
        var sourceEndpoint = run.Source;
        var transformer = sourceEndpoint != null && _endpointTransformers.TryGetValue(sourceEndpoint, out var epT)
            ? epT
            : _globalInputTransformer;

        if (transformer != null)
            return transformer.Transform(raw.AsSpan(), sourceEndpoint);

        try { return _serializer.Deserialize(raw.AsSpan()); }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to deserialize message from {Source}", sourceEndpoint);
            run.RecordFailure(ex);
            error = ex;
            return null;
        }
    }

    private static void StampReceived(IMessageContext context, ProcessingRun run)
    {
        context.Address ??= new EndpointAddress();
        context.Address.From = run.Source;
        run.Activity?.SetTag("messaging.message.id", context.Id);
        run.Activity?.SetTag("messaging.conversation_id", context.CorrelationId);
        run.Activity?.SetTag("messaging.message.type", context.MessageType);
    }

    /// <summary>Records the message's age; if it is older than the maximum age, logs it and returns the dead-letter description.</summary>
    private string? CheckExpired(IMessageContext context, string? sourceEndpoint)
    {
        var age = DateTime.UtcNow - context.Created;
        // A transformer may leave Created unset; negative ages are clock skew between hosts.
        if (context.Created != default)
            NymBrokerDiagnostics.MessageAge.Record(Math.Max(0, age.TotalSeconds),
                new KeyValuePair<string, object?>("source", sourceEndpoint));

        if (!(age > _maxMessageAge)) return null;

        var maxAge = _maxMessageAge.Value;
        _logger.LogWarning(
            "Discarding expired message {MessageId} (type={MessageType}, age={Age:F1}s, ttl={Ttl:F1}s)",
            context.Id, context.MessageType, age.TotalSeconds, maxAge.TotalSeconds);
        return $"Message age {age.TotalSeconds:F1}s exceeds the maximum of {maxAge.TotalSeconds:F1}s";
    }

    /// <summary>Runs the filters in order; null when one of them drops the message.</summary>
    private IMessageContext? ApplyFilters(IMessageContext context)
    {
        foreach (var filter in _filters)
        {
            if (filter.Filter(context) is not { } filtered) return null;
            context = filtered;
        }
        return context;
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
                return await DeadLetterAsync(raw, run, new DeadLetterCause(DeadLetterReasons.UnknownCompression, $"Unknown compression '{split.Compression}'", null), ct);
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
        var wasRouted = await RouteToDestinationsAsync(context, messageType, run.Source, ct);
        var (wasFannedOut, topicFailure) = await FanOutToTopicsAsync(raw, context, messageType, run, ct);

        if (wasRouted || wasFannedOut)
            return topicFailure ?? ProcessResult.Completed;

        if (messageType == null)
        {
            _logger.LogWarning("No consumer or route for unresolved message type '{MessageType}' from {Source}", context.MessageType, run.Source);
            return ProcessResult.Completed;
        }

        return await DispatchToConsumerAsync(raw, context, messageType, run, ct);
    }

    /// <summary>Dispatches to the message type's consumer; a failing consumer is settled by <see cref="SettlementFor"/>.</summary>
    private async ValueTask<ProcessResult> DispatchToConsumerAsync(byte[] raw, RawMessageContext context, Type messageType, ProcessingRun run, CancellationToken ct)
    {
        var message = MessageSerializerJson.DeserializeMessageObject(context, messageType);
        if (message == null) return ProcessResult.Completed;

        try
        {
            await _consumerDispatcher.DispatchAsync(messageType, message, context, ct);
            return ProcessResult.Completed;
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            run.RecordFailure(ex);
            var settlement = SettlementFor(run, retryOff: _noRetryMessageTypes.Contains(messageType));
            LogConsumerFailure(ex, messageType, settlement);
            return await SettleFailureAsync(settlement, raw, run, DeadLetterCause.Of(DeadLetterReasons.ConsumerFailed, ex), ct) ?? ProcessResult.Completed;
        }
    }

    private void LogConsumerFailure(Exception ex, Type messageType, FailureSettlement settlement)
    {
        switch (settlement)
        {
            case FailureSettlement.EndpointDeadLetters:
                _logger.LogError(ex, "Consumer failed for message type {MessageType} — retry is off for its consumer, so the endpoint dead-letters it", messageType.Name);
                break;
            case FailureSettlement.EndpointRetries:
                _logger.LogError(ex, "Consumer failed for message type {MessageType} — the endpoint will retry it", messageType.Name);
                break;
            default:
                _logger.LogError(ex, "Consumer failed for message type {MessageType} — routing to dead letter", messageType.Name);
                break;
        }
    }

    private static string ResultTag(ProcessOutcome outcome) => outcome switch
    {
        ProcessOutcome.Completed => "completed",
        ProcessOutcome.Retry => "retry",
        _ => "dead_letter"
    };

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
