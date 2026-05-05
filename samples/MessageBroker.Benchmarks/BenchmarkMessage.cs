using MessageBroker.Core.Message;

namespace MessageBroker.Benchmarks;

[MessageName("benchmark.message")]
public sealed record BenchmarkMessage(int Seq = 0, string Data = "");
