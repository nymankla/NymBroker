using MessageBroker.Core.Aggregator;
using MessageBroker.Core.Message;

namespace MessageBroker.Tests;

public sealed class AggregatorTests
{
    [Fact]
    public async Task AddAsync_ReturnsNull_UntilAllPartsReceived()
    {
        var agg = new AggregatorImpl();
        var ctx = new MessageContext<SplitMessage>();
        var corrId = Guid.NewGuid();
        var payload = "Hello, aggregated world!"u8.ToArray();
        var splitter = new Core.Splitter.SplitterImpl();
        var parts = splitter.Split(payload, new Core.Splitter.DefaultSplitCondition(10));

        Assert.True(parts.Count > 1, "Expected payload to be split into multiple parts.");

        byte[]? result = null;
        for (var i = 0; i < parts.Count - 1; i++)
            result = await agg.AddAsync(parts[i], ctx);

        Assert.Null(result);

        result = await agg.AddAsync(parts[^1], ctx);
        Assert.NotNull(result);
        Assert.Equal(payload, result);
    }
}
