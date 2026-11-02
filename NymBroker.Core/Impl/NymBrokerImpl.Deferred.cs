using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Resilience;

namespace NymBroker.Core.Impl;

// Deferred posts to an endpoint without IDeferredEndPoint: the broker holds the message in memory until it is due.
internal sealed partial class NymBrokerImpl
{
    // Guards the three fields below (never held across an await).
    private readonly Lock _heldLock = new();
    private readonly HashSet<Task> _heldMessages = [];
    private CancellationTokenSource _heldStop = new();
    private bool _heldStopped;

    // The caller has gone by the time the message is due, so the send gets some retries of its own: about 5 minutes in all.
    internal int HeldSendRetries { get; set; } = 10;
    internal TimeSpan HeldSendRetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    private void HoldUntilDue(string name, IEndPoint endpoint, byte[] message, DateTimeOffset deliverAt)
    {
        lock (_heldLock)
        {
            // Throwing lets a consumer that re-defers its message during shutdown have it redelivered instead of losing it.
            if (_heldStopped)
                throw new InvalidOperationException($"The broker has stopped, so it cannot hold a deferred message for endpoint '{name}'.");

            var token = _heldStop.Token;
            var held = Task.Run(() => DeliverWhenDueAsync(name, endpoint, message, deliverAt, token), CancellationToken.None);
            _heldMessages.Add(held);
            _ = held.ContinueWith(ForgetHeldMessage, CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default);
        }
    }

    private void ForgetHeldMessage(Task held)
    {
        lock (_heldLock)
            _heldMessages.Remove(held);
    }

    /// <summary>Fire-and-forget boundary of a held message: every way of not delivering it is logged.</summary>
    private async Task DeliverWhenDueAsync(string name, IEndPoint endpoint, byte[] message, DateTimeOffset deliverAt, CancellationToken token)
    {
        try
        {
            await DelayUntilAsync(deliverAt, token);

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
                        name, args.AttemptNumber + 1, args.Delay);
                    return ValueTask.CompletedTask;
                }
            });
            await policy.ExecuteAsync(async t => await endpoint.PostAsync(message, t), token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            _logger.LogWarning("A deferred message for {DeliverAt} on endpoint '{Name}' was dropped because the broker stopped", deliverAt, name);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "A deferred message for {DeliverAt} on endpoint '{Name}' was lost: it could not be sent", deliverAt, name);
        }
    }

    /// <summary>Allows holding messages again after a stop. A message posted before the start keeps waiting.</summary>
    private void StartHoldingMessages()
    {
        lock (_heldLock)
        {
            if (!_heldStopped) return;
            _heldStop.Dispose();
            _heldStop = new CancellationTokenSource();
            _heldStopped = false;
        }
    }

    /// <summary>
    /// Drops what is still waiting (each logs a warning) and waits for the rest of a send under way. The wait is limited,
    /// since an endpoint may ignore its token, and the broker has to stop anyway. Runs before the listeners stop; see <see cref="HoldUntilDue"/>.
    /// </summary>
    private async Task StopHoldingMessagesAsync()
    {
        CancellationTokenSource stop;
        Task[] held;
        lock (_heldLock)
        {
            _heldStopped = true;
            stop = _heldStop;
            held = [.. _heldMessages];
        }

        await stop.CancelAsync();
        try
        {
            await Task.WhenAll(held).WaitAsync(TimeSpan.FromSeconds(10), CancellationToken.None);
        }
        catch (TimeoutException ex)
        {
            _logger.LogError(ex, "A deferred message was still being sent 10 seconds after the broker began to stop; stopping anyway");
        }
    }
}
