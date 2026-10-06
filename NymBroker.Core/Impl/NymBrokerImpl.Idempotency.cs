using System.Diagnostics;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Idempotency;

namespace NymBroker.Core.Impl;

public sealed partial class NymBrokerImpl
{
    private IIdempotencyStore? _idempotencyStore;

    /// <summary>
    /// Makes the broker an idempotent receiver (configuration time): every decoded message is claimed in the store
    /// before routing and consumer dispatch, and completed or released afterwards (#55).
    /// </summary>
    public void SetIdempotencyStore(IIdempotencyStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        _idempotencyStore = store;
    }

    /// <summary>
    /// Runs <paramref name="process"/> under an idempotency claim on <paramref name="messageId"/>:
    /// <list type="bullet">
    /// <item><c>Claimed</c> → process; a <c>Retry</c> result (or an exception) releases the claim so the redelivery is
    /// processed, any other result completes it.</item>
    /// <item><c>Duplicate</c> → dropped, <c>Completed</c>.</item>
    /// <item><c>InProgress</c> → <c>Retry</c>: another delivery is being processed; the transport redelivers later.</item>
    /// <item>The store fails to claim → <c>Retry</c> (fail closed: never process without the duplicate check).</item>
    /// </list>
    /// </summary>
    private async Task<ProcessResult> ProcessIdempotentlyAsync(Guid messageId, string? sourceEndpoint,
        Action<Exception?> recordFailure, Func<Task<ProcessResult>> process, CancellationToken ct)
    {
        var store = _idempotencyStore;
        if (store is null)
            return await process();

        if (messageId == Guid.Empty)
        {
            // No ID to deduplicate on (e.g. an input transformer that does not set one).
            _logger.LogDebug("Message has no ID; idempotency check skipped");
            return await process();
        }

        IdempotencyClaimResult claim;
        try
        {
            claim = await store.TryClaimAsync(messageId, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            recordFailure(ex);
            _logger.LogError(ex, "Idempotency store failed to claim message {MessageId}; returning Retry so it is not processed without the duplicate check", messageId);
            return ProcessResult.Retry(ex);
        }

        switch (claim)
        {
            case IdempotencyClaimResult.Duplicate:
                _logger.LogDebug("Dropping duplicate message {MessageId} from {Source}", messageId, sourceEndpoint);
                NymBrokerDiagnostics.MessagesDuplicate.Add(1, new TagList { { "source", sourceEndpoint ?? "unknown" } });
                return ProcessResult.Completed;

            case IdempotencyClaimResult.InProgress:
                _logger.LogInformation("Message {MessageId} is already being processed; returning Retry", messageId);
                return ProcessResult.Retry(description: $"Message {messageId} is already being processed.");
        }

        ProcessResult result;
        try
        {
            result = await process();
        }
        catch
        {
            await SettleClaimAsync(store, messageId, complete: false);
            throw;
        }

        await SettleClaimAsync(store, messageId, complete: result.Outcome != ProcessOutcome.Retry);
        return result;
    }

    /// <summary>
    /// Completes or releases a claim. Uses no cancellation token: the message has been handled, so its outcome is
    /// recorded even while the broker is stopping. A failure is logged and leaves the lease to expire (worst case:
    /// one extra delivery).
    /// </summary>
    private async Task SettleClaimAsync(IIdempotencyStore store, Guid messageId, bool complete)
    {
        try
        {
            if (complete)
                await store.CompleteAsync(messageId, CancellationToken.None);
            else
                await store.ReleaseAsync(messageId, CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Idempotency store failed to {Action} message {MessageId}; the claim expires after its lease",
                complete ? "complete" : "release", messageId);
        }
    }
}
