# Benchmarks

[← Samples](samples.md) · [User guide](user-guide.md)

[`samples/NymBroker.Benchmarks`](../samples/NymBroker.Benchmarks) is a console app that measures end-to-end throughput and allocations for each endpoint and for the broker features that cost something per message (routing, wire tap, transformers, publish/subscribe, idempotency, dead-lettering, deferred posts, batching, splitting and compression). Use it to compare transports, to see what a feature costs, and to check a change for performance regressions.

It is a throughput harness, not a BenchmarkDotNet micro-benchmark: scenarios use a fixed message count, so expect some variation between runs. The fast in-process scenarios are measured five times and report the median run; the others run once.

## Running it

From the repository root, in Release:

```bash
dotnet run -c Release --project samples/NymBroker.Benchmarks
dotnet run -c Release --project samples/NymBroker.Benchmarks -- --filter idempotent   # only scenarios containing this text
```

The scenarios that need no service always run. The Postgres, SQL Server, MySQL, Azure Service Bus, RabbitMQ and SQS scenarios run only when that service is reachable on localhost, and are reported as `skipped` otherwise. Start the ones you want first (Docker Desktop required, see [Local infrastructure](samples.md#local-infrastructure)):

```powershell
./scripts/setup-postgres.ps1
./scripts/setup-sqlserver.ps1
./scripts/setup-mysql.ps1
./scripts/setup-servicebus.ps1    # uses the queue nymbroker.bench from scripts/servicebus/Config.json
./scripts/setup-rabbitmq.ps1
./scripts/setup-sqs.ps1           # ElasticMQ; the benchmark creates its queue nymbroker-bench
```

The benchmark drops and recreates its own tables (`nymbroker_bench` in PostgreSQL and MySQL, `dbo.nymbroker_bench`) and drains or purges its own queues (`nymbroker.bench`, `nymbroker-bench`) before each scenario, so it does not touch the samples' data.

## How a scenario is measured

For every scenario the runner:

1. Builds a fresh broker from [`benchmarksettings.json`](../samples/NymBroker.Benchmarks/benchmarksettings.json) (the Memory and File endpoints) plus the scenario's own endpoints, routes or topics, and starts it. Logging is set to `Error`, so log output does not skew the result.
2. Forces a full garbage collection and records the GC counts and the total allocated bytes.
3. Starts a stopwatch, posts (or publishes) all messages, and waits until a consumer or subscriber has signalled each one (`CompletionTracker`), with a 30 s limit — a scenario that misses it prints `[TIMEOUT]` and how many messages arrived.
4. Stops the stopwatch, reads the GC counters again and stops the broker.

So the time covers the whole round trip: serialize, send to the transport, receive, deserialize, route and dispatch to the consumer. The consumer itself does nothing but count, so the numbers show the broker's and the transport's overhead. A `drain backlog` scenario is the exception: it posts first, and the time covers only starting the broker and handling the messages.

The fast in-process scenarios finish in a few hundred milliseconds, so warm-up matters:

- Before any scenario, a full Memory scenario runs unmeasured. With only a short warm-up, the first scenarios measured ran up to twice as slow as the same scenario later in the process: GC budgets, the thread pool and the JIT-compiled broker code were still settling. The order of the Memory rows then decided which looked fastest.
- Each of them first sends unmeasured messages through its own broker (so its route, topic or transformer code is compiled), waits half a second for the optimized code, then does steps 2–4 five times and reports the median run (`InProcessWarmup` and `InProcessRuns` at the top of `Program.cs`).

## Scenarios

| Scenario | Messages | What it measures |
|---|---|---|
| Memory – direct | 50 000 | `PostAsync` to a Memory endpoint → consumer: the broker's own pipeline cost, and the row to compare the others with |
| Memory – + dashboard | 50 000 | `Memory – direct` with the [dashboard](dashboard.md)'s metric collector listening: what having the dashboard costs per message |
| Memory – 8 producers | 50 000 | The same, posted from eight tasks at once: contention |
| Memory – batch/100 | 50 000 | `PostBatchAsync` in batches of 100, without a transport that gains from it: the batch code path |
| Memory – 64 KB | 5 000 | A 64 KB message instead of a small one: how serialization scales with size |
| Memory – routed | 50 000 | A route that forwards every message to a second Memory endpoint |
| Memory – 20 routes | 50 000 | 19 routes that read the message and do not match, then one that does: the cost of evaluating conditions |
| Memory – wire tap | 50 000 | A copy of every message to a send-only endpoint with no transport |
| Input transformer | 50 000 | `PostRawAsync` of bare JSON turned into a message by an `IInputTransformer`. It skips serializing an envelope, so it can beat direct |
| Output transformer | 50 000 | A route to a send-only endpoint with an `IOutputTransformer` |
| PubSub – 3 subs | 50 000 | `PublishAsync` to a topic with three subscribers (every message is handled three times) |
| PubSub – endpoint | 50 000 | A topic that forwards to an endpoint, consumed there |
| Idempotent – memory | 50 000 | The default in-memory duplicate check on each message |
| Idempotent – SQLite / Postgres | 1 000 | A durable duplicate check on a Memory endpoint: all the time above direct is the store |
| Consumer fails → DLQ | 10 000 | Every consumer call throws and the message goes to the dead-letter endpoint: what a run of poison messages costs |
| Deferred – in broker | 20 000 | Deferred one second on an endpoint that cannot defer. Msg/sec mostly shows that wait; read Alloc/msg, the cost of holding a message |
| Split+Compress – direct | 200 | ~270 KB compressible messages split at 16 KB, Brotli-compressed first |
| Split – no compress | 200 | The same messages split without compression |
| Split+Compress – random | 200 | ~270 KB of Base64 random data, compressed first: what `compress` costs when it gains little |
| Split+Compress – zstd, Split+zstd – random | 200 | The two compressed rows above with `ZstandardCompressor` instead of the default Brotli |
| File – direct | 100 | File endpoint: one file written and picked up per message |
| HTTP – no network | 10 000 | HTTP endpoint with a handler that answers at once: what the endpoint and `HttpClient` add per request, without a server |
| SQLite – direct / batch/100 | 1 000 | SQLite queue table (in-memory database, `BatchSize = 100`), single posts and batches of 100 |
| SQLite / Postgres – 5 ms consumer, … ×8 | 400 | A consumer that waits 5 ms per message, as one doing I/O would, with `MaxConcurrentCalls` 1 and then 8 (`BatchSize` 100 and 50), draining a backlog like the `drain backlog` rows (posting one by one would feed the queue more slowly than eight handlers empty it). Shows what parallel handling gains when the consumer, not the transport, is the limit. `Task.Delay(5)` waits about 15 ms on Windows, so read the ratio between the two rows, not the absolute numbers |
| SQLite – deferred, Postgres – deferred | 1 000 | `PostDeferredAsync` with a time in the past: each message goes through the deferred table and is moved to the queue. The move runs about four times a second, so short SQLite runs show that interval |
| Postgres – direct / batch/100 | 1 000 | PostgreSQL queue table with single `PostAsync`, then `PostBatchAsync` in batches of 100 |
| SqlServer – direct / batch/100 | 1 000 | SQL Server queue table, single posts and batches of 100 |
| MySQL – direct / batch/100 | 1 000 | MySQL queue table (`BatchSize = 50`), single posts and batches of 100 |
| SQS – direct / batch/100 | 1 000 | Amazon SQS API on ElasticMQ (queue `nymbroker-bench`), single posts and batches of 100 |
| RabbitMQ – direct / batch/100 | 1 000 | RabbitMQ queue with the default durable publish, single posts, then `PostBatchAsync` in batches of 100 with `BatchAckSize = 100` |
| RabbitMQ – not durable | 1 000 | Single posts with `DurablePublish = false`: what durability costs |
| \<transport\> – drain backlog | 5 000 | SQLite, Postgres, SQL Server, MySQL, RabbitMQ and SQS: the backlog is posted first, then only handling it is measured. This is the receive side, without the send side limiting it |
| ServiceBus – direct | 200 | Azure Service Bus emulator queue (`PrefetchCount = 100`). The emulator limits throughput, so this only shows that the endpoint works and what it allocates |

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

- **In-process scenarios** show the broker's ceiling. The difference from `Memory – direct` is the cost of that feature. A route costs more than most, because every message goes through two Memory endpoints and is deserialized twice. Apart from `Input transformer`, if a feature looks *faster* than direct, the run was noisy, so run it again with fewer background processes.
- **Database and broker scenarios** are bound by the transport — durable commits and network round trips — not by NymBroker. Docker Desktop adds latency, so a native or production server is usually faster. The `batch/100` rows show what `PostBatchAsync` gains over one `PostAsync` per message; see [Sending messages](sending-messages.md). A `direct` row is usually limited by sending: compare it with `drain backlog` to see how fast the transport can be read.
- **Split scenarios** compare compression on and off, and Brotli with Zstandard. On a compressible payload, compression usually means fewer parts and fewer bytes on the wire, at some CPU cost. On random data it costs far more time and gains little, so pass `compress: false` for data that is already compressed or encrypted.
- Compare runs on the same machine, in Release, with the same services running. Close other heavy processes.

### Example run on .NET 11

One full run of the benchmark on .NET 11 (SDK 11.0.100), before the 0.25 release (November 2026): a laptop with an Intel Core Ultra 7 165H (22 logical cores, 64 GB RAM) on Windows 11, with every service in Docker Desktop 29 on the same machine. Use it to see the order of magnitude and the relations between rows, not as a target for your own hardware.

```
Scenario                        Msg/sec     Elapsed   Gen0   Gen1   Gen2     Alloc/msg
──────────────────────────────────────────────────────────────────────────────────────
Memory – direct                 285 714      175 ms     11      9      0    2,7 KB/msg
Memory – 8 producers            204 918      244 ms     12     11      0    3,1 KB/msg
Memory – batch/100              276 243      181 ms     10      9      0    2,7 KB/msg
Memory – 64 KB                   10 000      500 ms     76     52     20  258,7 KB/msg
Memory – routed                 208 333      240 ms     20     19      0    4,9 KB/msg
Memory – 20 routes              158 227      316 ms     20     19      0    4,9 KB/msg
Memory – wire tap               279 329      179 ms     11     10      0    2,7 KB/msg
Input transformer               310 559      161 ms      7      1      0    1,9 KB/msg
Output transformer              164 473      304 ms     16     15      0    4,0 KB/msg
PubSub – 3 subs                 263 157      190 ms      8      0      0    2,0 KB/msg
PubSub – endpoint               239 234      209 ms     16      1      0    4,0 KB/msg
Idempotent – memory             160 771      311 ms     12      2      0    3,0 KB/msg
Idempotent – SQLite               6 535      153 ms      0      0      0    6,0 KB/msg
Consumer fails → DLQ             33 670      297 ms     10      0      0   12,9 KB/msg
Deferred – in broker             16 570    1 207 ms      6      3      0    4,1 KB/msg
Split+Compress – direct             706      283 ms     74     74     74    3,2 MB/msg
Split – no compress                 518      386 ms     88     59     43    4,8 MB/msg
Split+Compress – random             102    1 942 ms    107     80     71    6,6 MB/msg
Split+Compress – zstd             1 273      157 ms     46     46     46    3,2 MB/msg
Split+zstd – random                 138    1 447 ms    107     82     72    6,7 MB/msg
File   – direct                     917      109 ms      0      0      0   15,8 KB/msg
HTTP – no network               108 695       92 ms      2      0      0    2,5 KB/msg
SQLite – direct                   6 578      152 ms      0      0      0    5,8 KB/msg
SQLite – batch/100               29 411       34 ms      0      0      0    4,0 KB/msg
SQLite – drain backlog           50 000      100 ms      1      0      0    2,9 KB/msg
SQLite – deferred                 3 496      286 ms      0      0      0    4,2 KB/msg
SQLite – 5 ms consumer               64    6 250 ms      0      0      0    4,5 KB/msg
SQLite – 5 ms consumer ×8           478      836 ms      0      0      0    4,7 KB/msg
Postgres – direct                   356    2 804 ms      1      0      0   13,8 KB/msg
Postgres – batch/100              4 424      226 ms      0      0      0    4,6 KB/msg
Postgres – drain backlog          9 328      536 ms      1      0      0    2,6 KB/msg
Postgres – deferred                 277    3 609 ms      0      0      0    9,2 KB/msg
Postgres – 5 ms consumer             62    6 408 ms      0      0      0    4,2 KB/msg
Postgres – 5 ms consumer ×8         420      951 ms      0      0      0    4,6 KB/msg
Idempotent – Postgres               127    7 856 ms      0      0      0    9,8 KB/msg
SqlServer – direct                   98   10 146 ms      1      0      0   14,4 KB/msg
SqlServer – batch/100             2 551      392 ms      0      0      0    5,5 KB/msg
SqlServer – drain backlog         2 972    1 682 ms      1      0      0    3,5 KB/msg
MySQL – direct                      100    9 917 ms      1      0      0   19,7 KB/msg
MySQL – batch/100                 1 312      762 ms      0      0      0    4,7 KB/msg
MySQL – drain backlog             1 840    2 717 ms      1      0      0    3,0 KB/msg
ServiceBus – direct                  36    5 423 ms      0      0      0   22,4 KB/msg
RabbitMQ – direct                   331    3 020 ms      0      0      0    5,3 KB/msg
RabbitMQ – batch/100              6 289      159 ms      0      0      0    4,6 KB/msg
RabbitMQ – not durable           10 000      100 ms      0      0      0    4,3 KB/msg
RabbitMQ – drain backlog         21 929      228 ms      1      0      0    3,1 KB/msg
SQS – direct                        321    3 106 ms      4      0      0   49,1 KB/msg
SQS – batch/100                   1 342      745 ms      1      0      0   12,3 KB/msg
SQS – drain backlog               1 234    4 050 ms      3      0      0    8,4 KB/msg
```

What it shows:

- **The broker's own cost** is a few microseconds and about 2.7 KB per message (`Memory – direct`); a route adds a second endpoint hop and deserialization (4.9 KB).
- **Zstandard** (`ZstandardCompressor`, built into .NET 11) handled split ~270 KB messages about 80 % faster than the default Brotli (1 273 against 706 msg/s), with fewer garbage collections at the same allocation. On random data the gain is smaller (138 against 102), and `compress: false` is still the better choice there. See [Large messages](sending-messages.md#large-messages-split-and-compress).
- **`MaxConcurrentCalls = 8`** multiplied the throughput of a consumer that waits 5 ms per message by about 7 (SQLite 64 → 478, PostgreSQL 62 → 420 msg/s).
- **Transports** are limited by the server, not by NymBroker: batching (`batch/100`) and reading a backlog (`drain backlog`) are 10–100 times faster than one durable post per message. The Service Bus emulator caps its row.

### Latency and failures (`--latency`)

```bash
dotnet run -c Release --project samples/NymBroker.Benchmarks -- --latency
```

This adds a second table built from the broker's own [metrics](observability.md#metrics). A `MeterListener` on the `NymBroker` and `NymBroker.Resilience` meters is active only during the measured runs, so warm-up is left out. For the in-process scenarios the figures come from the median run:

| Column | From | Meaning |
|---|---|---|
| Proc p50 / p99 | `nymbroker.message.processing.duration` | Time the broker spends on one message after the transport delivered it: decode, filters, routing and the consumer. It does not include the transport |
| Age p99 | `nymbroker.message.age` | Time from posting to the start of processing, which is how long messages waited in the transport. A high age next to a low processing time means the receive side of the transport is the bottleneck, not the broker |
| Failed / Retries | `nymbroker.messages.failed`, `nymbroker.retries` | Should be 0; if not, the run's throughput is not meaningful |

The listener runs a callback for every measurement, so `Msg/sec` is lower with `--latency`. Compare throughput only between runs without it. With `PublishAsync`, subscribers are called inline, so the PubSub rows show an age close to zero.

## Adding a scenario

Add a call to `RunAsync` in `Program.cs`. Its optional parameters cover most cases: `configureBuilder` to register endpoints, topics or routes (`b => b.AddRoute<T>(r => r.To("X"))`), `usePubSub` with `signalsPerMessage`, `messageFactory` for a different payload, `splitThresholdBytes` / `compress`, `postBatchSize` to post with `PostBatchAsync`, `producers` for several posting tasks, `bareInput` to post raw JSON, `consumerFails` to make the consumer throw, `deferBy` to post deferred, `drainBacklog` to measure only the receive side, `consumerDelay` for a consumer that waits per message, and `runs` / `warmup` for a median of several runs after a warm-up (use them for fast in-process scenarios). `AddSink` adds a send-only endpoint with no transport, which can signal the tracker. Messages are handled by `BenchmarkConsumer` (or the `BenchmarkSubscriber*` classes for topics), which signal the tracker. For an external service, add an availability check and a cleanup step like the existing Postgres, SQL Server, Service Bus and RabbitMQ helpers, so the scenario is skipped when the service is not running.
