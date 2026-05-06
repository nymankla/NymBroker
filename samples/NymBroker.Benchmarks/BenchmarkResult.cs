namespace NymBroker.Benchmarks;

public sealed record BenchmarkResult(
    string Name,
    int MessageCount,
    long ElapsedMs,
    int Gen0,
    int Gen1,
    int Gen2,
    long AllocatedBytes);
