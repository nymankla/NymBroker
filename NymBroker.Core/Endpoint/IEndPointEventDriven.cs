namespace NymBroker.Core.Endpoint;

public interface IEndPointEventDriven : IEndPoint
{
    Task StartListeningAsync(Func<string, CancellationToken, Task> handler, CancellationToken ct);
    Task StopListeningAsync();
}
