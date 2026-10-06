using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Impl;

internal sealed class ScheduledActionHandle : IAsyncDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly Task _task;
    private readonly ILogger _logger;

    public ScheduledActionHandle(CancellationTokenSource cts, Task task, ILogger logger)
    {
        _cts = cts;
        _task = task;
        _logger = logger;
    }

    /// <summary>Cancels the schedule and waits for its loop. Never throws because of the action (#51).</summary>
    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();

        try
        {
            await _task;
        }
        catch (OperationCanceledException)
        {
            // Clean shutdown.
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Scheduled action faulted while stopping");
        }
        finally
        {
            _cts.Dispose();
        }
    }
}
