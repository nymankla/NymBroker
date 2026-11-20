using Microsoft.Extensions.Logging;
using NymBroker.Core.Diagnostics;

namespace NymBroker.Core.Endpoint.Queue;

/// <summary>
/// What <see cref="INymBroker.GetQueueReplayer"/> returns: the endpoint's own replayer plus an audit trail. Every call is
/// logged at Warning, because it is an operator action that runs consumer side effects again, and the replayed messages are
/// counted in <c>nymbroker.messages.replayed</c>.
/// </summary>
internal sealed class AuditedQueueReplayer(string endpointName, IQueueReplayer inner, ILogger logger) : IQueueReplayer
{
    public async Task<QueueReplayResult> ReplayFailedAsync(IReadOnlyCollection<QueueMessageInfo> messages, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        QueueReplay.Validate(messages);
        QueueReplayResult result;
        try
        {
            result = await inner.ReplayFailedAsync(messages, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            logger.LogError(ex, "Replay of {Count} failed message(s) on {Endpoint} failed; nothing was replayed", messages.Count, endpointName);
            throw;
        }

        if (result.Replayed.Count > 0)
            NymBrokerDiagnostics.MessagesReplayed.Add(result.Replayed.Count, new KeyValuePair<string, object?>("endpoint", endpointName));
        logger.LogWarning("Replayed {ReplayedCount} failed message(s) on {Endpoint}: {Replayed}; skipped {SkippedCount}: {Skipped}",
            result.Replayed.Count, endpointName, result.Replayed, result.Skipped.Count, result.Skipped);
        return result;
    }
}

/// <summary>Argument checks for <see cref="IQueueReplayer.ReplayFailedAsync"/>, shared by the broker and its tests.</summary>
internal static class QueueReplay
{
    internal static void Validate(IReadOnlyCollection<QueueMessageInfo> messages)
    {
        ArgumentOutOfRangeException.ThrowIfZero(messages.Count, nameof(messages));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(messages.Count, IQueueInspector.MaxPageSize, nameof(messages));
        foreach (var message in messages)
        {
            if (message is not { State: QueueInspectionState.Failed, FailedAt: not null })
                throw new ArgumentException($"Message '{message?.Id}' is not a failed message with a failure time; only failed messages can be replayed.", nameof(messages));
        }
    }
}
