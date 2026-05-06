using NymBroker.Core.Aggregator;

namespace NymBroker.Core.Splitter;

public sealed class SplitterImpl : ISplitter
{
    public IReadOnlyList<SplitMessage> Split(byte[] data, ISplitCondition condition)
    {
        if (!condition.NeedsSplitting(data)) return [];

        var chunkSize = condition.MaxChunkSizeBytes;
        var totalChunks = (int)Math.Ceiling((double)data.Length / chunkSize);
        var correlationId = Guid.NewGuid();
        var parts = new List<SplitMessage>(totalChunks);

        for (var i = 0; i < totalChunks; i++)
        {
            var offset = i * chunkSize;
            var length = Math.Min(chunkSize, data.Length - offset);
            var chunk = new byte[length];
            Buffer.BlockCopy(data, offset, chunk, 0, length);

            parts.Add(new SplitMessage
            {
                CorrelationId = correlationId,
                CorrelationSequence = i,
                GroupSize = totalChunks,
                Body = Convert.ToBase64String(chunk)
            });
        }

        return parts;
    }
}
