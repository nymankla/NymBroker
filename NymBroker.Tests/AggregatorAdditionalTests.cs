using NymBroker.Core.Aggregator;
using NymBroker.Core.Message;
using Microsoft.Extensions.Logging.Abstractions;

namespace NymBroker.Tests;

public sealed class AggregatorAdditionalTests
{
    private static SplitMessage MakePart(Guid correlationId, int sequence, int groupSize, byte[] data)
        => new()
        {
            CorrelationId = correlationId,
            CorrelationSequence = sequence,
            GroupSize = groupSize,
            Body = Convert.ToBase64String(data)
        };

    [Fact]
    public async Task AddAsync_CompletesImmediately_WhenGroupSizeIsOne()
    {
        var agg = new AggregatorImpl(NullLogger<AggregatorImpl>.Instance);
        var ctx = new MessageContext<SplitMessage>();
        var payload = "single"u8.ToArray();
        var part = MakePart(Guid.NewGuid(), 0, 1, payload);

        var result = await agg.AddAsync(part, ctx, TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.Equal(payload, result);
    }

    [Fact]
    public async Task AddAsync_TwoIndependentAggregates_DoNotInterfere()
    {
        var agg = new AggregatorImpl(NullLogger<AggregatorImpl>.Instance);
        var ctx = new MessageContext<SplitMessage>();
        var corrId1 = Guid.NewGuid();
        var corrId2 = Guid.NewGuid();
        var payload1 = "first-payload"u8.ToArray();
        var payload2 = "second-payload"u8.ToArray();

        // Each split into 2 parts of 7 bytes
        var part1a = MakePart(corrId1, 0, 2, payload1[..7]);
        var part1b = MakePart(corrId1, 1, 2, payload1[7..]);
        var part2a = MakePart(corrId2, 0, 2, payload2[..7]);
        var part2b = MakePart(corrId2, 1, 2, payload2[7..]);

        // Interleave the parts from both aggregates
        Assert.Null(await agg.AddAsync(part1a, ctx, TestContext.Current.CancellationToken));
        Assert.Null(await agg.AddAsync(part2a, ctx, TestContext.Current.CancellationToken));

        var result1 = await agg.AddAsync(part1b, ctx, TestContext.Current.CancellationToken);
        var result2 = await agg.AddAsync(part2b, ctx, TestContext.Current.CancellationToken);

        Assert.NotNull(result1);
        Assert.NotNull(result2);
        Assert.Equal(payload1, result1);
        Assert.Equal(payload2, result2);
    }

    [Fact]
    public async Task AddAsync_ReassemblesPartsInOrder_WhenReceivedOutOfOrder()
    {
        var agg = new AggregatorImpl(NullLogger<AggregatorImpl>.Instance);
        var ctx = new MessageContext<SplitMessage>();
        var corrId = Guid.NewGuid();
        var chunk0 = "AAA"u8.ToArray();
        var chunk1 = "BBB"u8.ToArray();
        var chunk2 = "CCC"u8.ToArray();

        // Send in reverse order: parts 2, 1, 0
        Assert.Null(await agg.AddAsync(MakePart(corrId, 2, 3, chunk2), ctx, TestContext.Current.CancellationToken));
        Assert.Null(await agg.AddAsync(MakePart(corrId, 1, 3, chunk1), ctx, TestContext.Current.CancellationToken));
        var result = await agg.AddAsync(MakePart(corrId, 0, 3, chunk0), ctx, TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.Equal([.. chunk0, .. chunk1, .. chunk2], result);
    }

    [Fact]
    public async Task AddAsync_CustomCompletionCondition_IsRespected()
    {
        // Use a condition that requires exactly 3 parts regardless of GroupSize.
        var condition = new RequireThreePartsCondition();
        var agg = new AggregatorImpl(NullLogger<AggregatorImpl>.Instance, condition);
        var ctx = new MessageContext<SplitMessage>();
        var corrId = Guid.NewGuid();

        // GroupSize claims 2 but condition requires 3.
        Assert.Null(await agg.AddAsync(MakePart(corrId, 0, 2, "A"u8.ToArray()), ctx, TestContext.Current.CancellationToken));
        Assert.Null(await agg.AddAsync(MakePart(corrId, 1, 2, "B"u8.ToArray()), ctx, TestContext.Current.CancellationToken));
        var result = await agg.AddAsync(MakePart(corrId, 2, 2, "C"u8.ToArray()), ctx, TestContext.Current.CancellationToken);

        Assert.NotNull(result);
    }

    private sealed class RequireThreePartsCondition : ICompletionCondition
    {
        public bool IsComplete(SplitMessage incoming, Aggregate aggregate) => aggregate.ReceivedCount >= 3;
    }
}
