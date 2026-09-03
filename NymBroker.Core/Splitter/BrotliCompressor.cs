using System.IO.Compression;

namespace NymBroker.Core.Splitter;

/// <summary>
/// Default <see cref="ICompressor"/> — Brotli, built into .NET (no extra package) and typically
/// compresses text/JSON meaningfully better than GZip/Deflate at comparable speed.
/// </summary>
public sealed class BrotliCompressor(CompressionLevel level = CompressionLevel.Fastest) : ICompressor
{
    public string Name => "brotli";

    public byte[] Compress(byte[] data)
    {
        using var output = new MemoryStream();
        using (var brotli = new BrotliStream(output, level, leaveOpen: true))
            brotli.Write(data, 0, data.Length);
        return output.ToArray();
    }

    public byte[] Decompress(byte[] data)
    {
        using var input = new MemoryStream(data);
        using var brotli = new BrotliStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        brotli.CopyTo(output);
        return output.ToArray();
    }
}
