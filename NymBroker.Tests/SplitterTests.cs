using NymBroker.Core.Splitter;

namespace NymBroker.Tests;

public sealed class SplitterTests
{
    private readonly SplitterImpl _sut = new();

    [Fact]
    public void Split_SmallPayload_ReturnsEmpty()
    {
        var data = new byte[100];
        var parts = _sut.Split(data, new DefaultSplitCondition(512));
        Assert.Empty(parts);
    }

    [Fact]
    public void Split_LargePayload_ProducesCorrectChunks()
    {
        var data = new byte[250];
        new Random(42).NextBytes(data);
        var condition = new DefaultSplitCondition(100);
        var parts = _sut.Split(data, condition);

        Assert.Equal(3, parts.Count);
        Assert.All(parts, p => Assert.Equal(parts[0].CorrelationId, p.CorrelationId));
        Assert.Equal(3, parts[0].GroupSize);

        // Reassemble and verify integrity.
        var reassembled = parts
            .OrderBy(p => p.CorrelationSequence)
            .SelectMany(p => Convert.FromBase64String(p.Body))
            .ToArray();
        Assert.Equal(data, reassembled);
    }
}
