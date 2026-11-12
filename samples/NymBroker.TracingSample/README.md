# Distributed tracing sample

This sample shows W3C trace context crossing a process boundary. It uses a local SQLite queue so it needs no external services or collector.

Open two terminals in the repository root. Start the consumer in the first:

```bash
dotnet run --project samples/NymBroker.TracingSample
```

The consumer is the default mode, including when starting the project in Visual Studio. It waits until you press Ctrl+C; `--mode consumer` is also accepted. Wait for `Consumer listening for orders` before posting an order from the second terminal:

```bash
dotnet run --project samples/NymBroker.TracingSample -- --mode producer
```

The producer starts a simulated inbound request activity, then NymBroker creates a `nymbroker.send` span and writes its trace context into the queued envelope. The consumer process prints its `nymbroker.process` span. Compare the `trace=` values in both terminals: they are the same trace, while the spans and parent span IDs differ. The sample's `ActivityListener` prints spans directly to the console; use [OpenTelemetry exporters](../../docs/observability.md#enabling-collection) to send traces to a collector.

Both processes use the same SQLite database in the operating system's temporary directory. Run them on the same machine and under the same user account.
