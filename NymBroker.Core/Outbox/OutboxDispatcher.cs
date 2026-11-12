using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Impl;

namespace NymBroker.Core.Outbox;

/// <summary>
/// Sends committed outbox rows while the host runs: claims a batch, sends it in order through the broker (so output
/// transformers apply), and removes each row once sent. A failed send is retried with backoff and, after
/// <see cref="OutboxOptions.MaxAttempts"/>, left in the table as failed. Started after the broker and stopped before it.
/// </summary>
internal sealed partial class OutboxDispatcher(
    IOutboxStore store,
    NymBrokerImpl broker,
    OutboxOptions options,
    ILogger<OutboxDispatcher> logger) : BackgroundService
{
    // Writes after a send get their own token: a send that worked must be recorded even while the host stops.
    private static readonly TimeSpan BookkeepingTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan ReadFailureBackoff = TimeSpan.FromSeconds(5);
    private const int MaxErrorLength = 4000;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (true)
            {
                var wait = await DispatchOrBackOffAsync(stoppingToken);
                if (wait > TimeSpan.Zero)
                    await Task.Delay(wait, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host stopping.
        }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "The outbox dispatcher stopped unexpectedly; outbox messages are not sent until the application restarts");
        }
    }

    /// <summary>One batch; returns how long to wait before the next (none after a full batch).</summary>
    private async Task<TimeSpan> DispatchOrBackOffAsync(CancellationToken ct)
    {
        try
        {
            return await DispatchBatchAsync(ct) >= options.BatchSize ? TimeSpan.Zero : options.PollInterval;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            var wait = options.PollInterval > ReadFailureBackoff ? options.PollInterval : ReadFailureBackoff;
            logger.LogError(ex, "Reading the outbox failed; trying again in {Delay}", wait);
            return wait;
        }
    }

    /// <summary>Claims one batch and sends it in order. Returns how many rows it claimed.</summary>
    internal async Task<int> DispatchBatchAsync(CancellationToken ct)
    {
        var claimId = Guid.NewGuid();
        var batch = await store.ClaimAsync(claimId, options.BatchSize, options.LeaseTimeout, ct);
        for (var i = 0; i < batch.Count; i++)
        {
            if (await DispatchAsync(batch[i], claimId, ct)) continue;

            // Stopping: hand back what was not sent, so the next dispatcher need not wait for the lease.
            for (var j = i; j < batch.Count; j++)
                await BookkeepAsync(batch[j], "release", t => store.ReleaseAsync(batch[j].Id, claimId, t));
            break;
        }
        return batch.Count;
    }

    /// <summary>Sends one message and records the outcome. Returns false if the dispatcher is stopping and it was not sent.</summary>
    private async Task<bool> DispatchAsync(ClaimedOutboxMessage message, Guid claimId, CancellationToken ct)
    {
        if (ct.IsCancellationRequested) return false;

        if (!broker.CanSendTo(message.EndpointName))
        {
            var error = $"No writable endpoint named '{message.EndpointName}' is registered.";
            logger.LogError("Outbox message {Id} cannot be sent and stays in the outbox as failed: {Error}", message.Id, error);
            await BookkeepAsync(message, "mark as failed", t => store.MarkFailedAsync(message.Id, claimId, error, t));
            return true;
        }

        try
        {
            await broker.SendFromOutboxAsync(message, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            return false;
        }
        catch (Exception ex)
        {
            await RetryOrFailAsync(message, claimId, ex);
            return true;
        }

        await BookkeepAsync(message, "remove", t => store.MarkSentAsync(message.Id, claimId, t));
        LogSent(message.Id, message.EndpointName, message.Attempt);
        return true;
    }

    private async Task RetryOrFailAsync(ClaimedOutboxMessage message, Guid claimId, Exception ex)
    {
        var error = Truncate($"{ex.GetType().Name}: {ex.Message}");
        if (message.Attempt >= options.MaxAttempts)
        {
            logger.LogError(ex, "Outbox message {Id} for endpoint '{Endpoint}' could not be sent in {Attempts} attempts and stays in the outbox as failed",
                message.Id, message.EndpointName, message.Attempt);
            await BookkeepAsync(message, "mark as failed", t => store.MarkFailedAsync(message.Id, claimId, error, t));
            return;
        }

        var delay = options.RetryDelayAfter(message.Attempt);
        logger.LogWarning(ex, "Outbox message {Id} for endpoint '{Endpoint}' could not be sent (attempt {Attempt} of {MaxAttempts}); retrying in {Delay}",
            message.Id, message.EndpointName, message.Attempt, options.MaxAttempts, delay);
        await BookkeepAsync(message, "schedule a retry for", t => store.RetryLaterAsync(message.Id, claimId, delay, error, t));
    }

    /// <summary>Writes an outcome. A failure is logged: the row becomes due again when its lease expires, so nothing is lost.</summary>
    private async Task BookkeepAsync(ClaimedOutboxMessage message, string action, Func<CancellationToken, Task> write)
    {
        using var timeout = new CancellationTokenSource(BookkeepingTimeout);
        try
        {
            await write(timeout.Token);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Could not {Action} outbox message {Id} for endpoint '{Endpoint}'; it is due again when its lease expires",
                action, message.Id, message.EndpointName);
        }
    }

    private static string Truncate(string text) => text.Length <= MaxErrorLength ? text : text[..MaxErrorLength];

    [LoggerMessage(LogLevel.Debug, "Sent outbox message {Id} to endpoint '{Endpoint}' (attempt {Attempt})")]
    private partial void LogSent(long id, string endpoint, int attempt);
}
