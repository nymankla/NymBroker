namespace NymBroker.Core.Aggregator;

public sealed class SplitMessage
{
    public Guid CorrelationId { get; set; }
    public int CorrelationSequence { get; set; }
    public int GroupSize { get; set; }

    /// <summary>Base64-encoded chunk of the original payload.</summary>
    public string Body { get; set; } = string.Empty;

    /// <summary>
    /// Codec used to compress the payload before splitting (e.g. "brotli"), or null if the
    /// payload was not compressed. Set by <see cref="Splitter.ICompressor.Name"/>; the same
    /// value is shared by every part in the correlation group.
    /// </summary>
    public string? Compression { get; set; }
}
