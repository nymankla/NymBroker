namespace MessageBroker.Core.Splitter;

public sealed class DefaultSplitCondition : ISplitCondition
{
    public int MaxChunkSizeBytes { get; }

    public DefaultSplitCondition(int maxChunkSizeBytes = 64 * 1024)
        => MaxChunkSizeBytes = maxChunkSizeBytes;

    public bool NeedsSplitting(byte[] data) => data.Length > MaxChunkSizeBytes;
}
