namespace MessageBroker.Benchmarks;

public sealed class CompletionTracker
{
    private int _target;
    private int _remaining;
    private TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public int Processed => _target - Volatile.Read(ref _remaining);

    public Task Prepare(int count)
    {
        _target = count;
        _tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Volatile.Write(ref _remaining, count);
        return _tcs.Task;
    }

    public void Signal()
    {
        if (Interlocked.Decrement(ref _remaining) == 0)
            _tcs.TrySetResult();
    }
}
