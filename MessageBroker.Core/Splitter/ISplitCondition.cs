namespace MessageBroker.Core.Splitter;

public interface ISplitCondition
{
    bool NeedsSplitting(byte[] data);
    int MaxChunkSizeBytes { get; }
}
