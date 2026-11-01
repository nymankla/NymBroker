using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Endpoint.Queue;

/// <summary>
/// The listener loop of a lease-based queue table (SQLite, PostgreSQL, SQL Server, or your own pull transport).
/// Derive from it (typically as a private nested class of the endpoint) and supply the data access; the loop
/// handles everything else:
/// <list type="bullet">
/// <item>claims batches back to back while messages are waiting, and calls <see cref="WaitWhenIdleAsync"/> after an empty
/// poll or an error;</item>
/// <item>writes the outcomes of batch N in the same call that claims batch N+1 (<see cref="FinalizeAndClaimAsync"/>),
/// so an implementation can use one round trip and one commit per batch;</item>
/// <item>turns each <see cref="ProcessResult"/> into an outcome: Completed; DeadLetter → Failed at once; Retry (or a
/// handler exception) → Pending, or Failed once <see cref="ILeasedQueueSettings.MaxRetryCount"/> is reached;</item>
/// <item>on stop, writes the outcomes of messages already handled (<see cref="FinalizeAsync"/>, 10 s timeout); messages
/// claimed but not handled stay InProgress and are redelivered after their lease expires;</item>
/// <item>deletes old finished rows on <see cref="ILeasedQueueSettings.CleanupInterval"/>, beside the loop rather than in it,
/// so a slow delete doesn't hold up messages (<see cref="DeleteFinishedAsync"/>).</item>
/// </list>
/// Failures are logged, never swallowed: poll errors at Error (the loop continues), a dead loop at Critical,
/// a failed shutdown write at Warning.
/// </summary>
public abstract class LeasedQueueListener
{
    private static readonly TimeSpan ShutdownFinalizeTimeout = TimeSpan.FromSeconds(10);
    // Not on every claim: moving due messages must cost a busy queue nothing measurable.
    private static readonly TimeSpan PromotionInterval = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan PromotionRetryDelay = TimeSpan.FromSeconds(5);
    // What PeriodicTimer accepts.
    private static readonly TimeSpan MinCleanupInterval = TimeSpan.FromMilliseconds(1);
    private static readonly TimeSpan MaxCleanupInterval = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private CancellationTokenSource? _listeningCts;
    private Task? _loop;
    private Task? _cleanup;

    /// <param name="endpointName">The endpoint's name, used in log messages.</param>
    /// <param name="settings">Batch size, poll interval, lease and retry limit.</param>
    /// <param name="logger">The endpoint's logger, so log categories stay the endpoint's.</param>
    /// <exception cref="ArgumentException">The retention or cleanup settings are invalid.</exception>
    protected LeasedQueueListener(string endpointName, ILeasedQueueSettings settings, ILogger logger)
    {
        ValidateCleanupSettings(settings);
        EndpointName = endpointName;
        Settings = settings;
        Logger = logger;
    }

    protected string EndpointName { get; }
    protected ILeasedQueueSettings Settings { get; }
    protected ILogger Logger { get; }

    /// <summary>The delay after an empty poll: <see cref="ILeasedQueueSettings.PollInterval"/>, at least 1 ms.</summary>
    protected TimeSpan IdleDelay => Settings.PollInterval > TimeSpan.Zero ? Settings.PollInterval : TimeSpan.FromMilliseconds(1);

    /// <summary>The lease in whole seconds (rounded up, at least 1), as the claim statements take it.</summary>
    public static int GetLeaseTimeoutSeconds(ILeasedQueueSettings settings)
        => (int)Math.Max(1, Math.Ceiling(settings.LeaseTimeout.TotalSeconds));

    /// <summary>Starts the loop on the thread pool and returns. Throws if it is already running.</summary>
    public void Start(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (_loop is not null)
            throw new InvalidOperationException($"Endpoint '{EndpointName}' is already listening.");

        _listeningCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = _listeningCts.Token;
        _loop = Task.Run(() => RunAsync(handler, token), CancellationToken.None);

        var keepsEverything = Settings.CompletedRetention == TimeSpan.Zero && Settings.FailedRetention == TimeSpan.Zero;
        if (Settings.CleanupInterval > TimeSpan.Zero && !keepsEverything)
            _cleanup = Task.Run(() => RunCleanupAsync(token), CancellationToken.None);
    }

    /// <summary>Stops polling and waits for the loop to finish, so no handler runs after this returns.</summary>
    public async Task StopAsync()
    {
        _listeningCts?.Cancel();
        if (_loop is not null)
        {
            await _loop;
            _loop = null;
        }
        if (_cleanup is not null)
        {
            await _cleanup;
            _cleanup = null;
        }
        _listeningCts?.Dispose();
        _listeningCts = null;
    }

    /// <summary>
    /// Writes <paramref name="outcomes"/> (may be empty) and claims the next batch, ideally in one transaction. If it
    /// throws, the loop logs a poll error and passes the same outcomes again on the next call.
    /// </summary>
    protected abstract Task<IReadOnlyList<QueueMessage>> FinalizeAndClaimAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct);

    /// <summary>Writes <paramref name="outcomes"/> (never empty) without claiming; used when the loop stops.</summary>
    protected abstract Task FinalizeAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct);

    /// <summary>
    /// Waits after a poll that claimed nothing (or failed). Default: <see cref="IdleDelay"/>. Override to wake up
    /// earlier, e.g. on a database notification; must throw <see cref="OperationCanceledException"/> when
    /// <paramref name="ct"/> is cancelled, and should handle its own failures.
    /// </summary>
    protected virtual Task WaitWhenIdleAsync(CancellationToken ct) => Task.Delay(IdleDelay, ct);

    /// <summary>
    /// Moves up to <see cref="ILeasedQueueSettings.BatchSize"/> deferred messages that are due to the queue, in its own
    /// transaction, and returns how many. Called before a claim, about four times a second, or at once while it returns a
    /// full batch. A failure is logged and retried a few seconds later; the queue keeps working meanwhile. Default: nothing
    /// to move.
    /// </summary>
    protected virtual Task<int> PromoteDueAsync(CancellationToken ct) => Task.FromResult(0);

    /// <summary>
    /// Deletes up to <see cref="ILeasedQueueSettings.CleanupBatchSize"/> rows with this <paramref name="status"/>
    /// (<see cref="QueueMessageStatus.Completed"/> or <see cref="QueueMessageStatus.Failed"/>) that finished more than
    /// <paramref name="olderThanSeconds"/> ago, and returns how many. Called on <see cref="ILeasedQueueSettings.CleanupInterval"/>,
    /// and again at once while it returns a full batch. It runs at the same time as the listener loop and may run on several
    /// instances against one table. A failure is logged and retried at the next interval. Default: nothing to delete.
    /// </summary>
    protected virtual Task<int> DeleteFinishedAsync(QueueMessageStatus status, int olderThanSeconds, CancellationToken ct) => Task.FromResult(0);

    /// <summary>Called once when the loop ends, before the shutdown finalize; release resources the loop used here.</summary>
    protected virtual ValueTask OnLoopExitAsync() => ValueTask.CompletedTask;

    private async Task RunAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken token)
    {
        // Outcomes of handled messages not written yet; written together with the next claim.
        var outcomes = new List<QueueMessageOutcome>();
        var nextPromotion = 0L;
        try
        {
            while (!token.IsCancellationRequested)
            {
                var claimedCount = 0;
                try
                {
                    if (Environment.TickCount64 >= nextPromotion)
                        nextPromotion = await PromoteAndGetNextTimeAsync(token);

                    var batch = await FinalizeAndClaimAsync(outcomes, token);
                    outcomes.Clear();   // committed together with the claim
                    claimedCount = batch.Count;

                    foreach (var message in batch)
                    {
                        var result = await EndpointHandler.InvokeAsync(handler, message.Payload, Logger, EndpointName, token);
                        outcomes.Add(ToOutcome(message, result));
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;   // stopping mid-batch: unhandled messages are left to lease expiry
                }
                catch (Exception ex)
                {
                    // Rolled back, so 'outcomes' is written on the next attempt. If the leases expire first, the
                    // attempt-count guard in the finalize statement turns that late write into a no-op.
                    Logger.LogError(ex, "Poll error on endpoint '{Name}'", EndpointName);
                }

                // Drain without waiting while there is work; wait only when idle (or after an error).
                if (claimedCount == 0)
                    await WaitWhenIdleAsync(token);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        catch (Exception ex)
        {
            Logger.LogCritical(ex, "Listener loop for endpoint '{Name}' terminated unexpectedly", EndpointName);
        }

        try
        {
            await OnLoopExitAsync();
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Cleanup after the listener loop of endpoint '{Name}' failed", EndpointName);
        }

        await FinalizeOnShutdownAsync(outcomes);
    }

    private async Task<long> PromoteAndGetNextTimeAsync(CancellationToken token)
    {
        try
        {
            var moved = await PromoteDueAsync(token);
            return moved >= Settings.BatchSize ? 0 : Environment.TickCount64 + (long)PromotionInterval.TotalMilliseconds;
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !token.IsCancellationRequested)
        {
            Logger.LogError(ex, "Could not move due deferred messages to the queue on endpoint '{Name}'; ordinary messages are not affected", EndpointName);
            return Environment.TickCount64 + (long)PromotionRetryDelay.TotalMilliseconds;
        }
    }

    private async Task RunCleanupAsync(CancellationToken token)
    {
        using var timer = new PeriodicTimer(Settings.CleanupInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(token))
            {
                await DeleteAllFinishedAsync(QueueMessageStatus.Completed, Settings.CompletedRetention, token);
                await DeleteAllFinishedAsync(QueueMessageStatus.Failed, Settings.FailedRetention, token);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Stopping.
        }
    }

    private async Task DeleteAllFinishedAsync(QueueMessageStatus status, TimeSpan retention, CancellationToken token)
    {
        if (retention == TimeSpan.Zero) return;

        var olderThanSeconds = (int)Math.Ceiling(retention.TotalSeconds);
        try
        {
            var total = 0;
            int deleted;
            do
            {
                deleted = await DeleteFinishedAsync(status, olderThanSeconds, token);
                total += deleted;
            }
            while (deleted >= Settings.CleanupBatchSize);

            Logger.LogDebug("Deleted {Count} old {Status} row(s) on endpoint '{Name}'", total, status, EndpointName);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !token.IsCancellationRequested)
        {
            Logger.LogError(ex, "Deleting old {Status} rows on endpoint '{Name}' failed; retrying at the next interval. Messages are not affected", status, EndpointName);
        }
    }

    private static void ValidateCleanupSettings(ILeasedQueueSettings settings)
    {
        ValidateRetention(settings.CompletedRetention, nameof(settings.CompletedRetention));
        ValidateRetention(settings.FailedRetention, nameof(settings.FailedRetention));

        if (settings.CleanupInterval != TimeSpan.Zero
            && (settings.CleanupInterval < MinCleanupInterval || settings.CleanupInterval > MaxCleanupInterval))
            throw new ArgumentException($"CleanupInterval must be zero (no cleanup) or between {MinCleanupInterval} and {MaxCleanupInterval}.", nameof(settings));
        if (settings.CleanupBatchSize < 1)
            throw new ArgumentException("CleanupBatchSize must be at least 1.", nameof(settings));
    }

    private static void ValidateRetention(TimeSpan retention, string name)
    {
        if (retention < TimeSpan.Zero || retention.TotalSeconds > int.MaxValue)
            throw new ArgumentException($"{name} must be zero (keep forever) or positive, and at most {int.MaxValue} seconds.", name);
    }

    private QueueMessageOutcome ToOutcome(QueueMessage message, ProcessResult result)
    {
        switch (result.Outcome)
        {
            case ProcessOutcome.Completed:
                return QueueMessageOutcome.Completed(message);

            case ProcessOutcome.DeadLetter:
                Logger.LogWarning("Message {MessageId} on endpoint '{Name}' dead-lettered (marked Failed): {Failure}",
                    message.MessageId, EndpointName, result.FailureText);
                return new QueueMessageOutcome(message, QueueMessageStatus.Failed, result.FailureText);

            default:
                if (message.AttemptCount >= Settings.MaxRetryCount)
                {
                    Logger.LogWarning("Message {MessageId} on endpoint '{Name}' marked Failed after {Attempts} attempts (terminal state); last error: {Error}",
                        message.MessageId, EndpointName, message.AttemptCount, result.FailureText);
                    return new QueueMessageOutcome(message, QueueMessageStatus.Failed, result.FailureText);
                }
                return new QueueMessageOutcome(message, QueueMessageStatus.Pending, result.FailureText);
        }
    }

    /// <summary>The messages were handled, so record their outcomes even though the loop is stopping (fresh token).</summary>
    private async Task FinalizeOnShutdownAsync(List<QueueMessageOutcome> outcomes)
    {
        if (outcomes.Count == 0) return;

        try
        {
            using var cts = new CancellationTokenSource(ShutdownFinalizeTimeout);
            await FinalizeAsync(outcomes, cts.Token);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Could not record results of {Count} message(s) on endpoint '{Name}' during shutdown; they will be redelivered after their lease expires",
                outcomes.Count, EndpointName);
        }
    }
}
