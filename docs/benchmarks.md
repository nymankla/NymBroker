# Benchmarks

[← Samples](samples.md) · [User guide](user-guide.md)

[`samples/NymBroker.Benchmarks`](../samples/NymBroker.Benchmarks) is a console app that measures end-to-end throughput and allocations for each endpoint and for the main broker features (routing, filters, publish/subscribe, batching, splitting and compression). Use it to compare transports, to see what a feature costs, and to check a change for performance regressions.

It is a throughput harness, not a BenchmarkDotNet micro-benchmark: each scenario runs once with a fixed message count, so expect some variation between runs.

## Running it

From the repository root, in Release:

```bash
dotnet run -c Release --project samples/NymBroker.Benchmarks
```

The Memory, File, SQLite and split scenarios always run. The Postgres, SQL Server, Azure Service Bus and RabbitMQ scenarios run only when that service is reachable on localhost, and are reported as `skipped` otherwise. Start the ones you want first (Docker Desktop required, see [Local infrastructure](samples.md#local-infrastructure)):

```powershell
./scripts/setup-postgres.ps1
./scripts/setup-sqlserver.ps1
./scripts/setup-servicebus.ps1    # uses the queue nymbroker.bench from scripts/servicebus/Config.json
./scripts/setup-rabbitmq.ps1
```

The benchmark drops and recreates its own tables (`nymbroker_bench`, `dbo.nymbroker_bench`) and drains or purges its own queues (`nymbroker.bench`) before each scenario, so it does not touch the samples' data.

## How a scenario is measured

For every scenario the runner:

1. Builds a fresh broker from [`benchmarksettings.json`](../samples/NymBroker.Benchmarks/benchmarksettings.json) (the Memory and File endpoints) plus the scenario's own endpoints, routes, topics or filter, and starts it. Logging is set to `Error`, so log output does not skew the result.
2. Forces a full garbage collection and records the GC counts and the total allocated bytes.
3. Starts a stopwatch, posts (or publishes) all messages, and waits until a consumer or subscriber has signalled each one (`CompletionTracker`), with a 30 s limit — a scenario that misses it prints `[TIMEOUT]` and how many messages arrived.
4. Stops the stopwatch, reads the GC counters again and stops the broker.

So the time covers the whole round trip: serialize, send to the transport, receive, deserialize, route and dispatch to the consumer. The consumer itself does nothing but count, so the numbers show the broker's and the transport's overhead. A 1 000-message warm-up runs first so JIT compilation is not measured.

Before the scenarios it also runs a short `FileSystemWatcher` check (`OK (5/5)`): if the file system does not raise events, the File scenario cannot complete.

## Scenarios

| Scenario | Messages | What it measures |
|---|---|---|
| Memory – direct | 50 000 | `PostAsync` to a Memory endpoint → consumer: the broker's own pipeline cost |
| Memory – routed | 50 000 | Same, plus a route that forwards every message to a second Memory endpoint |
| Memory – filtered | 50 000 | Same as direct, with a pass-through `IMessageFilter` |
| PubSub – 1 sub | 50 000 | `PublishAsync` to a topic with one `ISubscribe<T>` subscriber |
| PubSub – 3 subs | 50 000 | A topic with three subscribers (every message is handled three times) |
| PubSub – endpoint | 50 000 | A topic that forwards to an endpoint, consumed there |
| File – direct | 100 | File endpoint: one file written and picked up per message |
| SQL – direct | 1 000 | SQLite queue table (in-memory database, `BatchSize = 100`) |
| Split+Compress – direct | 200 | ~270 KB messages split at 16 KB, Brotli-compressed first |
| Split – no compress | 200 | The same messages split without compression |
| Postgres – direct / batch/100 | 1 000 | PostgreSQL queue table with single `PostAsync`, then `PostBatchAsync` in batches of 100 |
| SqlServer – direct / batch/100 | 1 000 | SQL Server queue table, single posts and batches of 100 |
| ServiceBus – direct / batch/100 | 1 000 | Azure Service Bus emulator queue (`PrefetchCount = 100`), single posts and batches of 100 |
| RabbitMQ – direct / batch/100 | 1 000 | RabbitMQ queue, acking every message (`BatchAckSize = 1`) and then every 100 (`BatchAckSize = 100`) |

The database scenarios use `PollInterval = TimeSpan.Zero`, so they show drain speed rather than polling delay. The message counts are constants at the top of `Program.cs`; raise them for steadier numbers on fast transports.

## Reading the results

The run ends with a table like this (the numbers depend entirely on your machine and Docker setup):

```
Scenario                        Msg/sec     Elapsed   Gen0   Gen1   Gen2     Alloc/msg
──────────────────────────────────────────────────────────────────────────────────────
Memory – direct                 ...         ... ms    ...    ...    ...      ... KB/msg
...
```

| Column | Meaning |
|---|---|
| Msg/sec | Messages divided by elapsed time. For `PubSub – 3 subs` it counts messages published, not deliveries |
| Elapsed | Wall-clock time from the first post until the last message was handled |
| Gen0 / Gen1 / Gen2 | Garbage collections of each generation during the scenario. Gen2 collections are the expensive ones |
| Alloc/msg | Bytes allocated by the whole process per message (broker, transport client and benchmark) |

Things to keep in mind when comparing:

- **Memory scenarios** show the broker's ceiling. Differences between direct, routed, filtered and pub/sub are the cost of that feature.
- **Database and broker scenarios** are bound by the transport — durable commits and network round trips — not by NymBroker. Docker Desktop adds latency, so a native or production server is usually faster. The `batch/100` rows show what `PostBatchAsync` (one round trip per batch) gains over one `PostAsync` per message; see [Sending messages](sending-messages.md).
- **Split scenarios** compare compression on and off for a compressible payload: compression usually means fewer parts and fewer bytes on the wire, at some CPU cost.
- Compare runs on the same machine, in Release, with the same services running. Close other heavy processes.

## Adding a scenario

Add a call to `RunAsync` in `Program.cs`. Its optional parameters cover most cases: `configureBuilder` to register endpoints or topics, `configure` to add routes on the started broker, `addFilter`, `usePubSub` with `signalsPerMessage`, `messageFactory` for a different payload, `splitThresholdBytes` / `compress`, and `postBatchSize` to post with `PostBatchAsync`. Messages are handled by `BenchmarkConsumer` (or the `BenchmarkSubscriber*` classes for topics), which signal the tracker. For an external service, add an availability check and a cleanup step like the existing Postgres, SQL Server, Service Bus and RabbitMQ helpers, so the scenario is skipped when the service is not running.
