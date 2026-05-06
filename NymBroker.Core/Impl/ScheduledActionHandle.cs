namespace NymBroker.Core.Impl;

internal sealed class ScheduledActionHandle : IAsyncDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly Task _task;

    public ScheduledActionHandle(CancellationTokenSource cts, Task task)
    {
        _cts = cts;
        _task = task;
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();

        try
        {
            await _task;
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            _cts.Dispose();
        }
    }
}
