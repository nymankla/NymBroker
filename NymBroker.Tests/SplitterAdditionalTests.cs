using NymBroker.Core.Splitter;

namespace NymBroker.Tests;

public sealed class SplitterAdditionalTests
{
    private readonly SplitterImpl _sut = new();

    // --- DefaultSplitCondition ---

    [Fact]
    public void DefaultSplitCondition_DefaultThreshold_Is64KB()
    {
        var condition = new DefaultSplitCondition();
        Assert.Equal(64 * 1024, condition.MaxChunkSizeBytes);
    }

    [Fact]
    public void DefaultSplitCondition_NeedsSplitting_ReturnsFalse_WhenPayloadAtOrBelowThreshold()
    {
        var condition = new DefaultSplitCondition(100);
        Assert.False(condition.NeedsSplitting(new byte[100]));
        Assert.False(condition.NeedsSplitting(new byte[50]));
    }

    [Fact]
    public void DefaultSplitCondition_NeedsSplitting_ReturnsTrue_WhenPayloadExceedsThreshold()
    {
        var condition = new DefaultSplitCondition(100);
        Assert.True(condition.NeedsSplitting(new byte[101]));
    }

    // --- SplitterImpl ---

    [Fact]
    public void Split_EmptyPayload_ReturnsEmpty()
    {
        var parts = _sut.Split([], new DefaultSplitCondition(10));
        Assert.Empty(parts);
    }

    [Fact]
    public void Split_PayloadExactlyAtThreshold_ReturnsEmpty()
    {
        // data.Length == MaxChunkSizeBytes — NeedsSplitting returns false (strictly greater than).
        var data = new byte[100];
        var parts = _sut.Split(data, new DefaultSplitCondition(100));
        Assert.Empty(parts);
    }

    [Fact]
    public void Split_PayloadOneByteOverThreshold_ProducesTwoParts()
    {
        var data = new byte[101];
        var parts = _sut.Split(data, new DefaultSplitCondition(100));
        Assert.Equal(2, parts.Count);
    }

    [Fact]
    public void Split_AllPartsShareTheSameCorrelationId()
    {
        var data = new byte[300];
        var parts = _sut.Split(data, new DefaultSplitCondition(100));
        Assert.All(parts, p => Assert.Equal(parts[0].CorrelationId, p.CorrelationId));
    }

    [Fact]
    public void Split_GroupSize_EqualsPartCount()
    {
        var data = new byte[300];
        var parts = _sut.Split(data, new DefaultSplitCondition(100));
        Assert.All(parts, p => Assert.Equal(parts.Count, p.GroupSize));
    }

    [Fact]
    public void Split_CorrelationSequences_AreZeroBased_AndContiguous()
    {
        var data = new byte[300];
        var parts = _sut.Split(data, new DefaultSplitCondition(100));
        var sequences = parts.Select(p => p.CorrelationSequence).OrderBy(s => s).ToList();
        Assert.Equal(Enumerable.Range(0, parts.Count).ToList(), sequences);
    }

    [Fact]
    public void Split_LastChunkMayBeSmallerThanChunkSize()
    {
        // 250 bytes split by 100 → chunks of 100, 100, 50
        var data = new byte[250];
        new Random(42).NextBytes(data);
        var parts = _sut.Split(data, new DefaultSplitCondition(100));

        var lastChunkBytes = Convert.FromBase64String(parts[^1].Body);
        Assert.Equal(50, lastChunkBytes.Length);
    }

    [Fact]
    public void Split_ReassembledData_MatchesOriginal()
    {
        var data = new byte[500];
        new Random(7).NextBytes(data);
        var parts = _sut.Split(data, new DefaultSplitCondition(100));

        var reassembled = parts
            .OrderBy(p => p.CorrelationSequence)
            .SelectMany(p => Convert.FromBase64String(p.Body))
            .ToArray();

        Assert.Equal(data, reassembled);
    }
}
