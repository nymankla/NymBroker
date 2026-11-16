namespace NymBroker.Dashboard.Metrics;

/// <summary>
/// Counts values in logarithmic buckets: each bucket is 2^(1/4) (about 19 %) wider than the one before, from
/// <see cref="Smallest"/> up. Recording is one interlocked increment, so it can sit on the broker's hot path; a
/// percentile is accurate to about one bucket, which is plenty for a chart.
/// </summary>
internal sealed class LogHistogram
{
    internal const int BucketCount = 96;
    private const int BucketsPerDoubling = 4;

    /// <summary>The upper bound of the first bucket (10 µs when values are milliseconds). The last bucket ends near 168 s.</summary>
    internal const double Smallest = 0.01;

    private readonly long[] _counts = new long[BucketCount];

    public void Record(double value) => Interlocked.Increment(ref _counts[BucketOf(value)]);

    /// <summary>Moves the counts into <paramref name="target"/> (added to what it holds) and resets them.</summary>
    public void DrainInto(long[] target)
    {
        for (var i = 0; i < BucketCount; i++)
        {
            var count = Interlocked.Exchange(ref _counts[i], 0);
            if (count != 0) target[i] += count;
        }
    }

    internal static int BucketOf(double value)
    {
        if (!(value > Smallest)) return 0;   // also NaN and negatives
        var index = (int)Math.Ceiling(BucketsPerDoubling * Math.Log2(value / Smallest));
        return Math.Clamp(index, 0, BucketCount - 1);
    }

    internal static double UpperBound(int bucket) => Smallest * Math.Pow(2, (double)bucket / BucketsPerDoubling);

    /// <summary>The value below which <paramref name="quantile"/> of the counted values lie (a bucket's upper bound), or null when nothing was counted.</summary>
    internal static double? Percentile(long[] counts, double quantile)
    {
        long total = 0;
        foreach (var count in counts) total += count;
        if (total == 0) return null;

        var rank = (long)Math.Ceiling(quantile * total);
        long seen = 0;
        for (var i = 0; i < counts.Length; i++)
        {
            seen += counts[i];
            if (seen >= rank) return UpperBound(i);
        }
        return UpperBound(counts.Length - 1);
    }
}
