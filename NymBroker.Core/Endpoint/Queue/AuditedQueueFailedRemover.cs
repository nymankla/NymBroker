using Microsoft.Extensions.Logging;
using NymBroker.Core.Diagnostics;

namespace NymBroker.Core.Endpoint.Queue;

/// <summary>
/// What <see cref="INymBroker.GetQueueFailedRemover"/> returns: the endpoint's own remover plus an audit trail. Every call is
/// logged at Warning, because it is an operator action that deletes messages for good, and the removed messages are
/// counted in <c>nymbroker.messages.removed</c>.
/// </summary>
internal sealed class AuditedQueueFailedRemover(string endpointName, IQueueFailedRemover inner, ILogger logger) : IQueueFailedRemover
{
    public async Task<QueueRemoveResult> RemoveFailedAsync(IReadOnlyCollection<QueueMessageInfo> messages, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        QueueReplay.Validate(messages);
        QueueRemoveResult result;
        try
        {
            result = await inner.RemoveFailedAsync(messages, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            logger.LogError(ex, "Removal of {Count} failed message(s) on {Endpoint} failed; nothing was removed", messages.Count, endpointName);
            throw;
        }

        if (result.Removed.Count > 0)
            NymBrokerDiagnostics.MessagesRemoved.Add(result.Removed.Count, new KeyValuePair<string, object?>("endpoint", endpointName));
        logger.LogWarning("Removed {RemovedCount} failed message(s) on {Endpoint}: {Removed}; skipped {SkippedCount}: {Skipped}",
            result.Removed.Count, endpointName, result.Removed, result.Skipped.Count, result.Skipped);
        return result;
    }
}
