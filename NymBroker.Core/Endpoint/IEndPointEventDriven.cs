namespace NymBroker.Core.Endpoint;

public interface IEndPointEventDriven : IEndPoint
{
    Task StartListeningAsync(Func<byte[], CancellationToken, Task> handler, CancellationToken ct);
    Task StopListeningAsync();
}
