using System.Text;
using NymBroker.Core.Splitter;

namespace NymBroker.Tests;

public sealed class CompressorTests
{
    private readonly BrotliCompressor _sut = new();

    [Fact]
    public void Name_IsBrotli()
    {
        Assert.Equal("brotli", _sut.Name);
    }

    [Fact]
    public void Compress_ThenDecompress_ReproducesOriginalBytes()
    {
        var original = Encoding.UTF8.GetBytes(new string('x', 5000));
        var compressed = _sut.Compress(original);
        var decompressed = _sut.Decompress(compressed);

        Assert.Equal(original, decompressed);
    }

    [Fact]
    public void Compress_HighlyCompressiblePayload_ShrinksSignificantly()
    {
        var original = Encoding.UTF8.GetBytes(new string('x', 5000));
        var compressed = _sut.Compress(original);

        Assert.True(compressed.Length < original.Length / 2,
            $"Expected significant shrinkage for repetitive data, got {original.Length} -> {compressed.Length} bytes.");
    }

    [Fact]
    public void Compress_ThenDecompress_RoundTrips_RandomIncompressibleData()
    {
        var original = new byte[2000];
        new Random(42).NextBytes(original);

        var compressed = _sut.Compress(original);
        var decompressed = _sut.Decompress(compressed);

        Assert.Equal(original, decompressed);
    }

    [Fact]
    public void Compress_EmptyPayload_RoundTrips()
    {
        var compressed = _sut.Compress([]);
        var decompressed = _sut.Decompress(compressed);
        Assert.Empty(decompressed);
    }
}
