using NymBroker.Core.Message;

namespace NymBroker.Benchmarks;

[MessageName("benchmark.message")]
public sealed record BenchmarkMessage(int Seq = 0, string Data = "");
