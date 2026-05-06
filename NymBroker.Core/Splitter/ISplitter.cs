using NymBroker.Core.Aggregator;

namespace NymBroker.Core.Splitter;

public interface ISplitter
{
    /// <summary>
    /// Splits <paramref name="data"/> into <see cref="SplitMessage"/> parts.
    /// Returns an empty list if no splitting is needed.
    /// </summary>
    IReadOnlyList<SplitMessage> Split(byte[] data, ISplitCondition condition);
}
