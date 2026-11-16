using System.Diagnostics;
using NymBroker.Core.Diagnostics;
using NymBroker.Dashboard;
using NymBroker.Dashboard.Metrics;

namespace NymBroker.Tests.Dashboard;

public sealed class DashboardMetricsTests
{
    private static MetricCollector Collector(TimeSpan? history = null)
        => new(new NymBrokerDashboardOptions { History = history ?? TimeSpan.FromMinutes(1) });

    [Fact]
    public void LogHistogram_Buckets_GrowWithTheValue()
    {
        Assert.Equal(0, LogHistogram.BucketOf(0));
        Assert.Equal(0, LogHistogram.BucketOf(-5));
        Assert.Equal(0, LogHistogram.BucketOf(double.NaN));
        Assert.True(LogHistogram.BucketOf(1) < LogHistogram.BucketOf(10));
        Assert.Equal(LogHistogram.BucketCount - 1, LogHistogram.BucketOf(1e12));
    }

    [Theory]
    [InlineData(0.5, 50)]
    [InlineData(0.99, 99)]
    public void LogHistogram_Percentile_IsWithinOneBucket(double quantile, double expected)
    {
        var histogram = new LogHistogram();
        for (var i = 1; i <= 100; i++) histogram.Record(i);
        var counts = new long[LogHistogram.BucketCount];
        histogram.DrainInto(counts);

        var actual = LogHistogram.Percentile(counts, quantile)!.Value;
        Assert.InRange(actual, expected, expected * 1.2);
    }

    [Fact]
    public void LogHistogram_Percentile_IsNullWithoutValues()
    {
        Assert.Null(LogHistogram.Percentile(new long[LogHistogram.BucketCount], 0.5));
    }

    [Fact]
    public void Roll_MovesTheSecondIntoTheHistory_AndStartsAnEmptyOne()
    {
        using var collector = Collector();
        collector.Add(Counter.Received, 10, "Orders");
        collector.Add(Counter.Failed, 2, "Orders");
        collector.RecordDuration(4);
        collector.RecordAge(1.5);
        collector.RecordAge(0.5);

        collector.Roll(1000);
        collector.Roll(1001);

        var history = collector.History();
        Assert.Equal([1000L, 1001L], history.Select(p => p.T));
        Assert.Equal(10, history[0].Received);
        Assert.Equal(2, history[0].Failed);
        Assert.Equal(1.5, history[0].MaxAgeSeconds);
        Assert.InRange(history[0].P99Ms!.Value, 4, 4.8);
        Assert.Equal(0, history[1].Received);
        Assert.Null(history[1].P99Ms);
        Assert.Null(history[1].MaxAgeSeconds);
        Assert.Equal([1001L], collector.History(afterUnixSecond: 1000).Select(p => p.T));
    }

    [Fact]
    public void History_KeepsOnlyTheConfiguredSeconds()
    {
        using var collector = Collector(TimeSpan.FromMinutes(1));
        for (var second = 1; second <= 200; second++)
        {
            collector.Add(Counter.Received, 1, "Orders");
            collector.Roll(second);
        }

        var history = collector.History();
        Assert.Equal(60, history.Count);
        Assert.Equal(141, history[0].T);
        Assert.Equal(200, history[^1].T);
    }

    [Fact]
    public void Recent_AveragesRatesOverTheLastSeconds()
    {
        using var collector = Collector();
        foreach (var (second, received) in new[] { (1L, 100L), (2L, 10L), (3L, 20L) })
        {
            collector.Add(Counter.Received, received, "Orders");
            collector.Add(Counter.Consumed, received, null);
            collector.RecordDuration(received);
            collector.Roll(second);
        }

        var recent = collector.Recent(2);
        Assert.Equal(15, recent.Received);
        Assert.Equal(15, recent.Consumed);
        Assert.InRange(recent.P99Ms!.Value, 20, 24);

        // An endpoint first seen later is averaged over the same seconds as the broker.
        collector.Add(Counter.Received, 30, "Late");
        collector.Roll(4);
        Assert.Equal(15, collector.RecentFor("Late", 2).Received);
        Assert.Equal(0, collector.RecentFor("Unknown", 2).Received);
    }

    [Fact]
    public void Endpoints_AreTrackedUpToALimit_SoTagValuesCannotGrowMemory()
    {
        using var collector = Collector();
        for (var i = 0; i < 400; i++) collector.Add(Counter.Received, 1, $"endpoint-{i}");
        collector.Roll(1);

        Assert.Equal(1, collector.RecentFor("endpoint-0", 1).Received);
        Assert.Equal(0, collector.RecentFor("endpoint-399", 1).Received);
        Assert.Equal(400, collector.History()[0].Received);
    }

    [Fact]
    public void Start_ListensToTheBrokersOwnInstruments()
    {
        using var collector = Collector();
        collector.Start();
        var source = $"listener-{Guid.NewGuid():N}";

        NymBrokerDiagnostics.MessagesReceived.Add(3, new TagList { { "source", source } });
        NymBrokerDiagnostics.MessagesFailed.Add(1, new TagList { { "source", source } });
        collector.Roll(1);

        var rates = collector.RecentFor(source, 1);
        Assert.Equal(3, rates.Received);
        Assert.Equal(1, rates.Failed);
    }
}
