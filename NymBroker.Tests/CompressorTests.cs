using System.IO.Compression;
using System.Text;
using NymBroker.Core.Splitter;

namespace NymBroker.Tests;

public sealed class CompressorTests
{
    public static TheoryData<string> Codecs => ["brotli", "zstd"];

    private static ICompressor Create(string name) => name switch
    {
        "brotli" => new BrotliCompressor(),
        "zstd" => new ZstandardCompressor(),
        _ => throw new ArgumentOutOfRangeException(nameof(name))
    };

    [Theory]
    [MemberData(nameof(Codecs))]
    public void Name_IsTheWireCodecName(string codec)
    {
        Assert.Equal(codec, Create(codec).Name);
    }

    [Theory]
    [MemberData(nameof(Codecs))]
    public void Compress_ThenDecompress_ReproducesOriginalBytes(string codec)
    {
        var sut = Create(codec);
        var original = Encoding.UTF8.GetBytes(new string('x', 5000));

        Assert.Equal(original, sut.Decompress(sut.Compress(original)));
    }

    [Theory]
    [MemberData(nameof(Codecs))]
    public void Compress_HighlyCompressiblePayload_ShrinksSignificantly(string codec)
    {
        var original = Encoding.UTF8.GetBytes(new string('x', 5000));
        var compressed = Create(codec).Compress(original);

        Assert.True(compressed.Length < original.Length / 2,
            $"Expected significant shrinkage for repetitive data, got {original.Length} -> {compressed.Length} bytes.");
    }

    [Theory]
    [MemberData(nameof(Codecs))]
    public void Compress_ThenDecompress_RoundTrips_RandomIncompressibleData(string codec)
    {
        var sut = Create(codec);
        var original = new byte[2000];
        new Random(42).NextBytes(original);

        Assert.Equal(original, sut.Decompress(sut.Compress(original)));
    }

    [Theory]
    [MemberData(nameof(Codecs))]
    public void Compress_EmptyPayload_RoundTrips(string codec)
    {
        var sut = Create(codec);
        Assert.Empty(sut.Decompress(sut.Compress([])));
    }

    [Theory]
    [InlineData(CompressionLevel.Fastest)]
    [InlineData(CompressionLevel.Optimal)]
    [InlineData(CompressionLevel.SmallestSize)]
    public void Zstandard_EveryLevel_RoundTrips_AndIsReadableByZstandardStream(CompressionLevel level)
    {
        var original = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Range(0, 2000).Select(i => $"{{\"id\":{i},\"name\":\"item {i % 13}\"}},")));
        var compressed = new ZstandardCompressor(level).Compress(original);

        Assert.Equal(original, new ZstandardCompressor().Decompress(compressed));

        // A standard frame, so any Zstandard decoder (another language, a tool) can read the parts.
        using var stream = new ZstandardStream(new MemoryStream(compressed), CompressionMode.Decompress);
        using var output = new MemoryStream();
        stream.CopyTo(output);
        Assert.Equal(original, output.ToArray());
    }

    [Fact]
    public void Zstandard_SmallestSize_IsNotLargerThanFastest()
    {
        var original = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Range(0, 5000).Select(i => $"order {i} for customer {i % 97}; ")));

        Assert.True(new ZstandardCompressor(CompressionLevel.SmallestSize).Compress(original).Length
            <= new ZstandardCompressor(CompressionLevel.Fastest).Compress(original).Length);
    }

    [Fact]
    public void Zstandard_NoCompression_IsRejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ZstandardCompressor(CompressionLevel.NoCompression));
    }

    [Theory]
    [MemberData(nameof(Codecs))]
    public void Decompress_Garbage_Throws(string codec)
    {
        var garbage = "this is not a compressed frame at all"u8.ToArray();

        Assert.ThrowsAny<Exception>(() => Create(codec).Decompress(garbage));
    }
}
