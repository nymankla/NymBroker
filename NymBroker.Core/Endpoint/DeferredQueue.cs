using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Endpoint;

/// <summary>
/// Holds deferred items in memory, ordered by due time, and hands each to a delivery callback when it is due. One loop
/// waits for the earliest item, however many are waiting (#117); it starts with the first item, so items added before a
/// listener starts are kept. A due item is delivered on its own task, so a slow or retrying delivery holds up no other.
/// </summary>
/// <remarks>Not durable: what is still waiting when it stops is dropped through the drop callback.</remarks>
internal sealed class DeferredQueue<T>
{
    // SemaphoreSlim.WaitAsync rejects waits over int.MaxValue ms (about 24 days), so a far item is waited for in steps.
    private static readonly TimeSpan MaxWait = TimeSpan.FromDays(1);

    private readonly Func<T, CancellationToken, Task> _deliver;
    private readonly Action<T> _dropped;
    private readonly ILogger _logger;
    private readonly string _owner;

    // Guards every field below (never held across an await).
    private readonly Lock _lock = new();
    private readonly PriorityQueue<(T Item, DateTimeOffset DueAt), (DateTimeOffset DueAt, long Sequence)> _waiting = new();
    private readonly HashSet<Task> _delivering = [];
    // Wakes the loop when an item is added in front of the one it waits for.
    private readonly SemaphoreSlim _wake = new(0, 1);
    private CancellationTokenSource _stop = new();
    private Task? _loop;
    private long _sequence;
    private bool _stopped;

    /// <param name="deliver">Delivers a due item; logs its own failures. Its token is cancelled when the queue stops.</param>
    /// <param name="dropped">Logs an item that was still waiting when the queue stopped.</param>
    /// <param name="owner">Names the owner in log messages.</param>
    public DeferredQueue(Func<T, CancellationToken, Task> deliver, Action<T> dropped, ILogger logger, string owner)
    {
        _deliver = deliver;
        _dropped = dropped;
        _logger = logger;
        _owner = owner;
    }

    /// <summary>Items still waiting (not those being delivered).</summary>
    public int WaitingCount
    {
        get { lock (_lock) return _waiting.Count; }
    }

    /// <summary>Whether the loop is running; false before the first item and after a stop.</summary>
    internal bool IsRunning
    {
        get { lock (_lock) return _loop is not null; }
    }

    /// <summary>Holds <paramref name="item"/> until <paramref name="dueAt"/>. Returns false once the queue has stopped.</summary>
    public bool TryAdd(T item, DateTimeOffset dueAt)
    {
        lock (_lock)
        {
            if (_stopped) return false;

            var isFirst = !_waiting.TryPeek(out var first, out _) || dueAt < first.DueAt;
            _waiting.Enqueue((item, dueAt), (dueAt, _sequence++));

            if (_loop is null)
            {
                var token = _stop.Token;
                _loop = Task.Run(() => RunAsync(token), CancellationToken.None);
            }
            else if (isFirst && _wake.CurrentCount == 0)
            {
                _wake.Release();
            }
        }
        return true;
    }

    private async Task RunAsync(CancellationToken stop)
    {
        try
        {
            while (true)
            {
                var wait = DeliverDueItems(stop);
                await _wake.WaitAsync(wait, stop);
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "The deferred-message loop of '{Owner}' terminated unexpectedly; it restarts with the next deferred message", _owner);
            lock (_lock) _loop = null;
        }
    }

    /// <summary>Starts delivering every due item and returns how long to wait for the next one.</summary>
    private TimeSpan DeliverDueItems(CancellationToken stop)
    {
        lock (_lock)
        {
            var now = DateTimeOffset.UtcNow;
            while (_waiting.TryPeek(out var next, out _) && next.DueAt <= now)
            {
                _waiting.Dequeue();
                StartDelivery(next.Item, stop);
            }

            if (!_waiting.TryPeek(out var first, out _))
                return Timeout.InfiniteTimeSpan;

            var wait = first.DueAt - now;
            // Rounded up: WaitAsync truncates to whole milliseconds and would spin until the item is due.
            return wait < MaxWait ? TimeSpan.FromMilliseconds(Math.Ceiling(wait.TotalMilliseconds)) : MaxWait;
        }
    }

    // Called under _lock.
    private void StartDelivery(T item, CancellationToken stop)
    {
        var delivery = Task.Run(() => _deliver(item, stop), CancellationToken.None);
        _delivering.Add(delivery);
        _ = delivery.ContinueWith(DeliveryFinished, CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default);
    }

    private void DeliveryFinished(Task delivery)
    {
        lock (_lock) _delivering.Remove(delivery);
        if (delivery.IsFaulted)
            _logger.LogError(delivery.Exception, "A deferred message of '{Owner}' was lost: its delivery failed", _owner);
    }

    /// <summary>Accepts items again after <see cref="StopAsync"/>. Does nothing if the queue has not stopped.</summary>
    public void Restart()
    {
        lock (_lock)
        {
            if (!_stopped) return;
            _stop.Dispose();
            _stop = new CancellationTokenSource();
            _stopped = false;
        }
    }

    /// <summary>
    /// Refuses new items, drops every waiting item through the drop callback, cancels deliveries under way and waits for
    /// them up to <paramref name="deliveryTimeout"/>. Returns false if one was still running then.
    /// </summary>
    public async Task<bool> StopAsync(TimeSpan deliveryTimeout)
    {
        CancellationTokenSource stop;
        Task? loop;
        lock (_lock)
        {
            if (_stopped) return true;
            _stopped = true;
            stop = _stop;
            loop = _loop;
            _loop = null;
        }

        await stop.CancelAsync();
        if (loop is not null) await loop;   // never throws: RunAsync logs

        (T Item, DateTimeOffset DueAt)[] dropped;
        Task[] delivering;
        lock (_lock)
        {
            dropped = [.. _waiting.UnorderedItems.Select(static entry => entry.Element)];
            _waiting.Clear();
            delivering = [.. _delivering];
        }

        foreach (var (item, _) in dropped)
            _dropped(item);

        return await WaitForDeliveriesAsync(delivering, deliveryTimeout);
    }

    private static async Task<bool> WaitForDeliveriesAsync(Task[] delivering, TimeSpan timeout)
    {
        var all = Task.WhenAll(delivering);
        try
        {
            await all.WaitAsync(timeout, CancellationToken.None);
        }
        catch (TimeoutException)
        {
            return false;
        }
        catch (Exception) when (all.IsFaulted)
        {
            // Each failed delivery was already logged by DeliveryFinished.
        }
        return true;
    }
}
