using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Endpoint;

/// <summary>Helpers for endpoint listeners that call the broker's handler.</summary>
public static class EndpointHandler
{
    /// <summary>
    /// Calls <paramref name="handler"/> for one received message. An exception is logged at Error and returned as
    /// <see cref="ProcessResult.Retry"/>, so the endpoint settles the message like any other failure. Only an
    /// <see cref="OperationCanceledException"/> while <paramref name="ct"/> is cancelled (the endpoint stopping) is rethrown;
    /// a cancellation raised by the handler itself, such as an HTTP timeout, is a failure.
    /// </summary>
    /// <param name="messageId">Optional transport message id, included in the log message.</param>
    public static async Task<ProcessResult> InvokeAsync(
        Func<byte[], CancellationToken, Task<ProcessResult>> handler, byte[] payload, ILogger logger, string endpointName,
        CancellationToken ct, object? messageId = null)
    {
        try
        {
            return await handler(payload, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            if (messageId is null)
                logger.LogError(ex, "Unhandled error dispatching message on endpoint '{Name}'", endpointName);
            else
                logger.LogError(ex, "Unhandled error dispatching message {MessageId} on endpoint '{Name}'", messageId, endpointName);
            return ProcessResult.Retry(ex);
        }
    }
}
