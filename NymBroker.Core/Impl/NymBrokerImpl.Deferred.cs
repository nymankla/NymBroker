using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Resilience;

namespace NymBroker.Core.Impl;

// Deferred posts to an endpoint without IDeferredEndPoint: the broker holds the message in memory until it is due.
internal sealed partial class NymBrokerImpl
{
    private readonly record struct HeldMessage(string Name, IEndPoint Endpoint, byte[] Message, DateTimeOffset DeliverAt);

    // One loop for every held message (#117), created on first use since it needs the logger.
    private DeferredQueue<HeldMessage>? _heldMessages;

    // The caller has gone by the time the message is due, so the send gets some retries of its own: about 5 minutes in all.
    internal int HeldSendRetries { get; set; } = 10;
    internal TimeSpan HeldSendRetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    private DeferredQueue<HeldMessage> HeldMessages
        => LazyInitializer.EnsureInitialized(ref _heldMessages,
            () => new DeferredQueue<HeldMessage>(DeliverHeldMessageAsync, LogDroppedHeldMessage, _logger, "broker"));

    /// <summary>Number of messages the broker holds for an endpoint that cannot defer (tests).</summary>
    internal int HeldMessageCount => _heldMessages?.WaitingCount ?? 0;

    private void HoldUntilDue(string name, IEndPoint endpoint, byte[] message, DateTimeOffset deliverAt)
    {
        // Throwing lets a consumer that re-defers its message during shutdown have it redelivered instead of losing it.
        if (!HeldMessages.TryAdd(new HeldMessage(name, endpoint, message, deliverAt), deliverAt))
            throw new InvalidOperationException($"The broker has stopped, so it cannot hold a deferred message for endpoint '{name}'.");
    }

    private void LogDroppedHeldMessage(HeldMessage held)
        => _logger.LogWarning("A deferred message for {DeliverAt} on endpoint '{Name}' was dropped because the broker stopped", held.DeliverAt, held.Name);

    /// <summary>Sends a held message that is due. Every way of not delivering it is logged.</summary>
    private async Task DeliverHeldMessageAsync(HeldMessage held, CancellationToken token)
    {
        try
        {
            var policy = new RetryPolicy(new RetryOptions
            {
                MaxRetryAttempts = HeldSendRetries,
                Delay = HeldSendRetryDelay,
                MaxDelay = TimeSpan.FromMinutes(1),
                BackoffType = RetryBackoffType.Exponential,
                ShouldHandle = static _ => true,   // an endpoint's own timeout is a failed send; the policy stops once the token is cancelled
                OnRetry = args =>
                {
                    _logger.LogWarning(args.Exception, "Could not send a deferred message to endpoint '{Name}' (attempt {Attempt}); retrying in {Delay}",
                        held.Name, args.AttemptNumber + 1, args.Delay);
                    return ValueTask.CompletedTask;
                }
            });
            await policy.ExecuteAsync(async t => await held.Endpoint.PostAsync(held.Message, t), token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            LogDroppedHeldMessage(held);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "A deferred message for {DeliverAt} on endpoint '{Name}' was lost: it could not be sent", held.DeliverAt, held.Name);
        }
    }

    /// <summary>Allows holding messages again after a stop. A message posted before the start keeps waiting.</summary>
    private void StartHoldingMessages() => _heldMessages?.Restart();

    /// <summary>
    /// Drops what is still waiting (each logs a warning) and waits for the rest of a send under way. The wait is limited,
    /// since an endpoint may ignore its token, and the broker has to stop anyway. Runs before the listeners stop; see <see cref="HoldUntilDue"/>.
    /// </summary>
    private async Task StopHoldingMessagesAsync()
    {
        if (!await HeldMessages.StopAsync(TimeSpan.FromSeconds(10)))
            _logger.LogError("A deferred message was still being sent 10 seconds after the broker began to stop; stopping anyway");
    }
}
