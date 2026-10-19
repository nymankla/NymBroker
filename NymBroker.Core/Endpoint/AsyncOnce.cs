namespace NymBroker.Core.Endpoint;

/// <summary>
/// Runs an asynchronous initialization once, such as creating an endpoint's table. Concurrent callers wait for the
/// running attempt; a failed attempt is not remembered, so the next caller tries again. Once it has succeeded,
/// <see cref="RunAsync{TState}"/> returns a completed task without locking.
/// </summary>
public sealed class AsyncOnce
{
    private readonly SemaphoreSlim _lock = new(1, 1);
    private volatile bool _completed;

    /// <summary>True once an attempt has succeeded.</summary>
    public bool IsCompleted => _completed;

    /// <summary>
    /// Runs <paramref name="action"/> unless an earlier call already completed it. Pass what the action needs as
    /// <paramref name="state"/> and use a <c>static</c> lambda, so a call after completion allocates nothing.
    /// </summary>
    public Task RunAsync<TState>(TState state, Func<TState, CancellationToken, Task> action, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(action);
        return _completed ? Task.CompletedTask : RunLockedAsync(state, action, ct);
    }

    private async Task RunLockedAsync<TState>(TState state, Func<TState, CancellationToken, Task> action, CancellationToken ct)
    {
        await _lock.WaitAsync(ct);
        try
        {
            if (_completed) return;
            await action(state, ct);
            _completed = true;
        }
        finally
        {
            _lock.Release();
        }
    }
}
