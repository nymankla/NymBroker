namespace NymBroker.Core.Aggregator;

public sealed class Aggregate
{
    private readonly SortedDictionary<int, byte[]> _parts = new();

    public Guid CorrelationId { get; }
    public int ReceivedCount => _parts.Count;
    public DateTime FirstReceived { get; } = DateTime.UtcNow;
    internal bool IsCompleted { get; set; }

    public Aggregate(Guid correlationId) => CorrelationId = correlationId;

    public void Add(SplitMessage msg)
    {
        var bytes = Convert.FromBase64String(msg.Body);
        _parts[msg.CorrelationSequence] = bytes;
    }

    public byte[] Reassemble()
    {
        var totalLength = _parts.Values.Sum(b => b.Length);
        var result = new byte[totalLength];
        var offset = 0;
        foreach (var chunk in _parts.Values)
        {
            Buffer.BlockCopy(chunk, 0, result, offset, chunk.Length);
            offset += chunk.Length;
        }
        return result;
    }
}
