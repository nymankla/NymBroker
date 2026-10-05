namespace NymBroker.Core.Endpoint;

public interface IEndPointEventDriven : IEndPoint
{
    /// <summary>
    /// Starts receiving in the background. The broker's handler returns a <see cref="ProcessResult"/> telling the
    /// endpoint how to settle each message (complete, let the transport redeliver, or dead-letter natively).
    /// </summary>
    Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct);

    Task StopListeningAsync();

    /// <summary>
    /// True when the transport can dead-letter a received message itself (and the endpoint's settings enable it).
    /// The broker then returns <see cref="ProcessOutcome.Retry"/> / <see cref="ProcessOutcome.DeadLetter"/> for failures
    /// instead of posting the message to its own dead-letter endpoint.
    /// </summary>
    bool UsesNativeDeadLetter => false;
}
