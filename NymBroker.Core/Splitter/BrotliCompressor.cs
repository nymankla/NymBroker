using System.IO.Compression;

namespace NymBroker.Core.Splitter;

/// <summary>
/// Brotli <see cref="ICompressor"/>, built into .NET (no extra package); it compresses text/JSON
/// meaningfully better than GZip/Deflate at comparable speed. The default up to NymBroker 0.25; since
/// 0.26 <see cref="ZstandardCompressor"/> is. Register it
/// (<c>services.AddSingleton&lt;ICompressor, BrotliCompressor&gt;()</c>) for receivers on 0.24 or earlier,
/// which cannot decode <c>zstd</c> parts.
/// </summary>
public sealed class BrotliCompressor(CompressionLevel level = CompressionLevel.Fastest) : ICompressor
{
    // Matches BrotliStream's default window (BrotliUtils.WindowBits_Default in the runtime source).
    private const int WindowBits = 22;

    public string Name => "brotli";

    public byte[] Compress(byte[] data)
    {
        var quality = QualityFromLevel(level);
        var buffer  = new byte[BrotliEncoder.GetMaxCompressedLength(data.Length)];

        if (!BrotliEncoder.TryCompress(data, buffer, out var bytesWritten, quality, WindowBits))
            throw new InvalidOperationException("Brotli compression failed.");

        if (bytesWritten == buffer.Length)
            return buffer;

        var result = new byte[bytesWritten];
        buffer.AsSpan(0, bytesWritten).CopyTo(result);
        return result;
    }

    // Mirrors BrotliUtils.GetQualityFromCompressionLevel (internal to System.IO.Compression) so
    // switching to the span-based encoder keeps the same quality-per-level behavior as before.
    private static int QualityFromLevel(CompressionLevel level) => level switch
    {
        CompressionLevel.NoCompression => 0,
        CompressionLevel.Fastest       => 1,
        CompressionLevel.SmallestSize  => 11,
        CompressionLevel.Optimal       => 4,
        _                              => 4
    };

    public byte[] Decompress(byte[] data)
    {
        using var input = new MemoryStream(data);
        using var brotli = new BrotliStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        brotli.CopyTo(output);
        return output.ToArray();
    }
}
