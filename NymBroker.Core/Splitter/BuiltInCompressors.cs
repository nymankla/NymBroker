namespace NymBroker.Core.Splitter;

/// <summary>
/// The codecs every receiver can decode, whichever <see cref="ICompressor"/> it sends with, so a
/// sender can switch codec before or after its receivers do.
/// </summary>
internal static class BuiltInCompressors
{
    private static readonly BrotliCompressor Brotli = new();
    private static readonly ZstandardCompressor Zstandard = new();

    /// <summary>The compressor that decodes <paramref name="name"/>: the configured one when its name matches, else a built-in codec, else null.</summary>
    public static ICompressor? ForDecoding(string name, ICompressor configured) =>
        string.Equals(name, configured.Name, StringComparison.Ordinal) ? configured
        : string.Equals(name, Zstandard.Name, StringComparison.Ordinal) ? Zstandard
        : string.Equals(name, Brotli.Name, StringComparison.Ordinal) ? Brotli
        : null;
}
