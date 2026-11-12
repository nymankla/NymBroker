using System.Diagnostics;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Message;

namespace NymBroker.Core.Impl;

internal sealed partial class NymBrokerImpl
{
    /// <summary>How a failed topic delivery or consumer is settled (the table in docs/writing-an-endpoint.md, "Native dead-lettering").</summary>
    private enum FailureSettlement
    {
        /// <summary>Native dead letter and retry is off: the transport dead-letters it now.</summary>
        EndpointDeadLetters,

        /// <summary>Native dead letter: the transport redelivers it and dead-letters natively once its delivery limit is reached.</summary>
        EndpointRetries,

        /// <summary>No native dead letter: the broker posts it to its dead-letter endpoint.</summary>
        BrokerDeadLetters,
    }

    private static FailureSettlement SettlementFor(ProcessingRun run, bool retryOff)
        => !run.NativeDeadLetter ? FailureSettlement.BrokerDeadLetters
            : retryOff ? FailureSettlement.EndpointDeadLetters
            : FailureSettlement.EndpointRetries;

    /// <summary>Why a message is dead-lettered: what <see cref="ProcessResult.DeadLetter"/> and <see cref="DeadLetterInfo"/> carry.</summary>
    private readonly record struct DeadLetterCause(string Reason, string? Description, Exception? Exception)
    {
        public static DeadLetterCause Of(string reason, Exception ex) => new(reason, ex.Message, ex);
    }

    /// <summary>
    /// The result for a failure settled as <paramref name="settlement"/>; null when the broker posted it to its
    /// dead-letter endpoint or none is configured.
    /// </summary>
    private async Task<ProcessResult?> SettleFailureAsync(FailureSettlement settlement, byte[] raw, ProcessingRun run,
        DeadLetterCause cause, CancellationToken ct)
    {
        switch (settlement)
        {
            case FailureSettlement.EndpointDeadLetters:
                RecordDeadLettered(cause.Reason, run.Source, "native");
                return ProcessResult.DeadLetter(cause.Reason, cause.Description, cause.Exception);
            case FailureSettlement.EndpointRetries:
                return ProcessResult.Retry(cause.Exception);
            default:
                return await TryPostToDeadLetterAsync(raw, run.Source, cause, ct);
        }
    }

    private bool UsesNativeDeadLetter(string? sourceEndpoint)
        => sourceEndpoint != null
           && _endpoints.TryGetValue(sourceEndpoint, out var endpoint)
           && endpoint is IEndPointEventDriven { UsesNativeDeadLetter: true };

    /// <summary>
    /// A message that can never succeed: native endpoints get <see cref="ProcessOutcome.DeadLetter"/>; otherwise it is
    /// posted to the broker's dead-letter endpoint (if configured) and reported as Completed, or Retry if that post failed.
    /// </summary>
    private async Task<ProcessResult> DeadLetterAsync(byte[] raw, ProcessingRun run, DeadLetterCause cause, CancellationToken ct)
    {
        if (run.NativeDeadLetter)
        {
            _logger.LogWarning("Message from {Source} is dead-lettered by the endpoint ({Reason}: {Description})",
                run.Source, cause.Reason, cause.Description);
            RecordDeadLettered(cause.Reason, run.Source, "native");
            return ProcessResult.DeadLetter(cause.Reason, cause.Description, cause.Exception);
        }

        return await TryPostToDeadLetterAsync(raw, run.Source, cause, ct) ?? ProcessResult.Completed;
    }

    private static void RecordDeadLettered(string reason, string? sourceEndpoint, string mode)
        => NymBrokerDiagnostics.MessagesDeadLettered.Add(1, new TagList
        {
            { "reason", reason },
            { "source", sourceEndpoint ?? "unknown" },
            { "mode", mode }
        });

    /// <summary>Returns null if the message was posted or no dead-letter endpoint is configured, otherwise the Retry that keeps it.</summary>
    private async Task<ProcessResult?> TryPostToDeadLetterAsync(byte[] raw, string? sourceEndpoint, DeadLetterCause cause, CancellationToken ct)
    {
        if (_deadLetterEndpoint == null) return null;
        if (!_endpoints.TryGetValue(_deadLetterEndpoint, out var dlq))
        {
            _logger.LogError("Dead letter endpoint '{Endpoint}' not found", _deadLetterEndpoint);
            return ProcessResult.Retry(description: $"Dead letter endpoint '{_deadLetterEndpoint}' not found.");
        }
        using var activity = StartSendActivity(_deadLetterEndpoint, context: null);
        try
        {
            var info = new DeadLetterInfo(cause.Reason, cause.Description, cause.Exception?.GetType().FullName, sourceEndpoint, DateTime.UtcNow);
            var envelope = DeadLetterEnvelope.AnnotateWithTrace(raw, info, TraceContext.Outgoing(_tracing.PropagateTraceContext));
            await SendAsync(_deadLetterEndpoint, dlq, envelope, ct);
            RecordDeadLettered(cause.Reason, sourceEndpoint, "broker");
            _logger.LogWarning("Message dead-lettered to endpoint '{Endpoint}' ({Reason}: {Description})", _deadLetterEndpoint, cause.Reason, cause.Description);
            return null;
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            RecordSendFailure(activity, ex);
            _logger.LogError(ex, "Failed to post message to dead letter endpoint '{Endpoint}'; returning Retry to the source endpoint", _deadLetterEndpoint);
            return ProcessResult.Retry(ex);
        }
    }
}
