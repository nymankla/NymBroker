namespace NymBroker.Core.Endpoint;

public interface IEndPointPoll : IEndPoint
{
    IAsyncEnumerable<string> ReadAsync(CancellationToken ct = default);
}
