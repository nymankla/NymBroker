namespace NymBroker.Core.Splitter;

/// <summary>
/// Compresses/decompresses the raw bytes of an oversized message before it is split into
/// <see cref="Aggregator.SplitMessage"/> parts. <see cref="Name"/> is stamped onto each part's
/// <see cref="Aggregator.SplitMessage.Compression"/> field so the receiving side knows how to
/// decode the reassembled bytes.
/// </summary>
public interface ICompressor
{
    /// <summary>Codec identifier stored in <see cref="Aggregator.SplitMessage.Compression"/>.</summary>
    string Name { get; }

    byte[] Compress(byte[] data);
    byte[] Decompress(byte[] data);
}
