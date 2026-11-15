using System.IO.Compression;

namespace NymBroker.Core.Splitter;

/// <summary>
/// Zstandard <see cref="ICompressor"/>, built into .NET 11 (no extra package). Compared with
/// <see cref="BrotliCompressor"/> at <see cref="CompressionLevel.Fastest"/> it compresses text/JSON
/// faster and to a similar size, and decompresses about three times as fast.
/// </summary>
/// <remarks>
/// Every receiver from NymBroker 0.25 on decompresses both <c>zstd</c> and <c>brotli</c> parts,
/// whichever compressor it uses itself. A receiver on 0.24 or earlier dead-letters <c>zstd</c> parts
/// as <c>UnknownCompression</c>, so switch senders only once every receiver is upgraded.
/// </remarks>
public sealed class ZstandardCompressor : ICompressor
{
    private readonly int _quality;

    /// <param name="level">
    /// <see cref="CompressionLevel.Fastest"/> (default, quality 1), <see cref="CompressionLevel.Optimal"/> (3)
    /// or <see cref="CompressionLevel.SmallestSize"/> (19) — the qualities <see cref="ZstandardStream"/> uses.
    /// <see cref="CompressionLevel.NoCompression"/> is rejected: pass <c>compress: false</c> when posting instead.
    /// </param>
    public ZstandardCompressor(CompressionLevel level = CompressionLevel.Fastest)
    {
        _quality = level switch
        {
            CompressionLevel.Fastest      => 1,
            CompressionLevel.Optimal      => 3,
            CompressionLevel.SmallestSize => 19,
            _ => throw new ArgumentOutOfRangeException(nameof(level), level,
                "Use Fastest, Optimal or SmallestSize; to send uncompressed, post with compress: false.")
        };
    }

    public string Name => "zstd";

    public byte[] Compress(byte[] data)
    {
        ArgumentNullException.ThrowIfNull(data);
        var buffer = new byte[ZstandardEncoder.GetMaxCompressedLength(data.Length)];

        if (!ZstandardEncoder.TryCompress(data, buffer, out var bytesWritten, _quality, ZstandardCompressionOptions.DefaultWindowLog2))
            throw new InvalidOperationException("Zstandard compression failed.");

        return bytesWritten == buffer.Length ? buffer : buffer.AsSpan(0, bytesWritten).ToArray();
    }

    /// <summary>
    /// Decompresses through a stream rather than allocating the size the frame header claims, since
    /// the bytes come from the transport and a crafted header could ask for an arbitrary allocation.
    /// </summary>
    public byte[] Decompress(byte[] data)
    {
        ArgumentNullException.ThrowIfNull(data);
        using var input = new MemoryStream(data);
        using var zstd = new ZstandardStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        zstd.CopyTo(output);
        return output.ToArray();
    }
}
