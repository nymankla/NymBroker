# Changelog

Notable changes to NymBroker are documented here. This changelog follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.16.0] - 2026-10-30

### Added

- `AddConsumer<TConsumer>(o => o.RetryOnFailure = false)`: a message whose consumer throws is dead-lettered at once (reason `ConsumerFailed`) instead of being returned for retry, when it came from an endpoint with its own dead-letter queue (the SQL queue tables, RabbitMQ, Service Bus). Messages from other endpoints already went straight to the broker's dead-letter endpoint. The option is per consumer, applies to all its message types, and does not affect topic subscribers or route destinations. Registering the same consumer again with a different value throws.

## [0.15.1] - 2026-10-30

### Fixed

- `StopAsync` threw `OperationCanceledException` without stopping anything when its token was already cancelled, for example when the host's shutdown timeout had passed before NymBroker's hosted service was stopped. The listeners kept running. `StopAsync` now always stops the scheduled actions and every listener; the token no longer aborts stopping.
- When one endpoint's `StopListeningAsync` threw, `StopAsync` did not stop the endpoints after it, and the broker still counted as started, so a later `StopAsync` did nothing and `StartAsync` did not restart them. Now each failure is logged at `Error`, every other endpoint is still stopped and the broker counts as stopped; then the failure is rethrown (several failures as one `AggregateException`).

### Changed

- Docs: scheduled actions, cron syntax and named actions with schedule stores moved from `docs/pipeline-extensions.md` to their own page, [`docs/scheduling.md`](docs/scheduling.md), linked from the user guide and the README.

## [0.15.0] - 2026-10-29

### Added

- Named scheduled actions: `AddScheduledAction(name, interval | cron, (services, ct) => …, o => o.MissedRuns = MissedRunPolicy.RunOnce)`. The broker claims each occurrence in a schedule store before it runs it, so instances that share the store run it once, and with `MissedRunPolicy.RunOnce` an action that missed occurrences while the app was down runs once right away on start (`MaxCatchUp` limits how old a missed run may be). A claim counts as the run, even if the action fails. Named interval actions run on a fixed grid (multiples of the interval since 1970-01-01 UTC) so instances agree on occurrences. Unnamed actions are unchanged.
- `IScheduleStore` and `NymBrokerBuilder.AddScheduleStore(factory)`; without a registered store, named actions use an in-process store.
- New packages with durable schedule stores: `NymBroker.Scheduling.Sqlite` (`AddSqliteScheduleStore`, one host), `NymBroker.Scheduling.Postgres` (`AddPostgresScheduleStore`), `NymBroker.Scheduling.SqlServer` (`AddSqlServerScheduleStore`) and `NymBroker.Scheduling.MySql` (`AddMySqlScheduleStore`). One row per action name, created automatically.
- Client skill `nymbroker-scheduled-recovery`: sets up named scheduled actions with a schedule store.

### Fixed

- A cron scheduled action whose next occurrence was more than about 49 days away (e.g. `@yearly`) stopped with a `Critical` log, because `Task.Delay` rejects such a delay. The wait now happens in steps of at most one day.
- The SQL Server and PostgreSQL idempotency stores could fail their first claim when several instances started at the same time and raced to create the table (SQL Server: "an index … already exists"; PostgreSQL: duplicate key on `pg_type`). The broker returned `Retry`, so no message was lost. SQL Server now ignores the "already exists" errors of a concurrent create, and PostgreSQL serializes the creation with a transaction-scoped advisory lock.

### Changed

- `samples/NymBroker.EfCoreTransactionSample`: an out-of-stock order is now rejected as a business outcome: the consumer rolls back to a savepoint, records an `OrderRejections` row in the same transaction and completes the message, instead of throwing so the queue retries it `MaxRetryCount` times and marks it `Failed`. Unexpected errors still throw and are retried.

## [0.14.0] - 2026-10-20

### Added

- `IEndPoint.HealthCheckAsync(CancellationToken)` and `HealthCheckResult.FromProbeAsync(...)`. `CheckHealthAsync` (and `/health`) now calls the async check, so a slow database or broker no longer holds a thread-pool thread for up to 5 s per endpoint. The default implementation calls `HealthCheck()`, so existing endpoints keep working unchanged. The SQLite, PostgreSQL, SQL Server, MySQL, SQS, Service Bus and HTTP endpoints implement it; their `HealthCheck()` now waits for it.
- Builder methods `AddRoute<T>(r => …)`, `AddRoute(r => …)`, `AddRoute(routeContextFactory, r => …)`, `AddFilter(filter)`, `AddFilter<TFilter>()`, and `AddScheduledAction(TimeSpan | cron, (services, ct) => …)`. A scheduled action is async, gets a new DI scope for every run and the broker's stop token. A route without `.To(...)` and an invalid cron expression throw when they are added.

### Changed

- **Breaking:** the broker is configured only on the builder (`AddNymBroker()`). `INymBroker` keeps the runtime operations (post, publish, process, start/stop, health), and `NymBrokerImpl` is now internal. Before, routes, filters and the dead-letter endpoint could be changed on a running broker, which skipped the startup checks (#81, #83). Migration:

  | Before (on `INymBroker`) | Now (on the builder) |
  |---|---|
  | `broker.Route<T>().To("X").When(…).Build()` | `.AddRoute<T>(r => r.To("X").When(…))` |
  | `broker.Route().To("X").Build()` | `.AddRoute(r => r.To("X"))` |
  | `broker.Route(() => new MyContext()).To("X").Build()` | `.AddRoute(() => new MyContext(), r => r.To("X"))` |
  | `broker.AddFilter(new MyFilter())` | `.AddFilter(new MyFilter())` or `.AddFilter<MyFilter>()` |
  | `broker.AddScheduledAction<INymBroker>(interval, b => b.PostAsync(…).GetAwaiter().GetResult(), broker)` | `.AddScheduledAction(interval, (sp, ct) => sp.GetRequiredService<INymBroker>().PostAsync(…, ct))` (same for a cron string) |
  | `broker.SetDeadLetterEndpoint("X")` / `AddWireTap("X")` / `SetMaxMessageAge(t)` | `.WithDeadLetterEndpoint("X")` / `.AddWireTap("X")` / `.DiscardMessagesOlderThan(t)` |
  | `broker.AddInputTransformer(instance, "X")` | `.AddInputTransformer<T>("X")` |
  | `new NymBrokerImpl(...)` | `services.AddNymBroker()…Build()` and resolve `INymBroker` |

  Scheduled actions can no longer be added after the broker is built.
- Dependencies: `Microsoft.Extensions.*`, `Microsoft.Data.Sqlite` 10.0.13, `AWSSDK.SQS` 4.0.100.19, `Microsoft.SourceLink.GitHub` 10.0.402.

## [0.13.0] - 2026-10-19

### Security

- `NymBroker.Endpoint.SqlServer` and `NymBroker.Idempotency.SqlServer` reference `System.Security.Cryptography.Pkcs` 10.0.13 directly. `Microsoft.Data.SqlClient` 7.1.1 (the latest) pulls in 9.0.18, which is affected by CVE-2026-84008 (high) and CVE-2026-70162 (moderate), both .NET denial-of-service vulnerabilities. The pin will be removed once SqlClient references a patched version.

### Added

- Output transformers (#83): `IOutputTransformer` and `AddOutputTransformer<T>(endpoint)` convert every message sent to an endpoint into the destination's own format, e.g. JSON → XML for a legacy system. They apply to `PostAsync`, `PostBatchAsync`, routes, topics and wire-tap or dead-letter endpoints. A null result sends nothing. Splitting a message to such an endpoint throws, and `StartAsync` throws if the broker would also listen on it without an input transformer. `samples/NymBroker.CsvSample` now also writes its orders as XML.

### Fixed

- A reassembled split message was counted as one more received message and processing time, and was copied twice (bytes → string → bytes) before processing. Now only the parts are counted, and the reassembled bytes are processed directly.

### Changed

- A message sent to several routes, or to several subscriber endpoints of a topic, is serialized once instead of once per destination.
- The broker's per-message log lines ("Routed message …", "Topic '…' delivered …", "Splitting message …") are logged at `Debug` instead of `Information`. At normal throughput they flooded production logs; the `nymbroker.messages.routed` metric counts the same events. Set the `NymBroker` category to `Debug` to see them again.
- The SQL endpoints and idempotency stores create their tables through one shared `AsyncOnce` helper (`NymBroker.Core.Endpoint`), instead of six hand-written lock-and-flag copies, some of which took the lock on every operation. `AsyncOnce` is public for endpoint and store authors.

### Removed

- **Breaking:** `IRouteBuilder<T>.Transform(fileName)` and `RouteContext.Transform`, obsolete no-ops since #48. Migration: delete the call; it never changed the message. To send a routed message in another format, use an output transformer on the destination.

## [0.12.1] - 2026-10-13

### Changed

- NuGet package tags and descriptions: shared tags for messaging and Enterprise Integration Patterns, plus per-package tags for the transport or database (`rabbitmq`, `azure-service-bus`, `sqs`, `postgresql`, …), so the packages show up in NuGet search.

## [0.12.0] - 2026-10-12

### Added

- Metric `nymbroker.message.age` (histogram, seconds, tag `source`): how old each message is when the broker starts processing it, measured from the envelope's `created`. It rises when a backlog builds, on every transport. Queue depth is still not measured by the broker; [docs/observability.md](docs/observability.md) lists where to read it for each transport.
- `samples/NymBroker.Benchmarks -- --latency`: a second results table from the broker's own metrics, with processing time p50/p99, message age p99, failures and retries per scenario ([docs/benchmarks.md](docs/benchmarks.md)).

### Changed

- **Breaking:** `StartAsync` now throws `InvalidOperationException` when a route, topic, dead-letter (`WithDeadLetterEndpoint`) or wire-tap target is not a registered endpoint (#81). Before, the broker started and the mistake only showed up later: a dead-lettered message was logged as "not found" and lost, and a route to an unknown endpoint was skipped with a warning. Migration: fix the endpoint name, or register the endpoint. The read-only messages were reworded at the same time (e.g. `Route target 'X' is read-only and cannot receive messages.`).

## [0.11.0] - 2026-10-12

### Added

- `NymBroker.Endpoint.Http`: a send-only HTTP / webhook endpoint (#77) — one request per message (`POST`, `PUT` or `PATCH`), the envelope or only its payload, `Idempotency-Key` and `X-NymBroker-*` headers, optional HMAC-SHA256 signature (`SigningSecret`), retries for timeouts, network errors, 408, 429 (honouring `Retry-After`) and 5xx, optional health-check URL, `AddHttpEndPoint` / `WithHttp()` (config type `Http`) on a named `IHttpClientFactory` client. A permanent failure such as 400 throws, so a route destination makes the source retry and then dead-letter the message.
- `samples/NymBroker.WebhookSample`: sending and receiving webhooks — signature verification with replay protection, a durable inbox, duplicate removal and an input transformer for another system's JSON.
- `NymBroker.Endpoint.AwsSqs`: an Amazon SQS endpoint (#75) — standard and FIFO queues, long polling, batched delete, `PostBatchAsync` via `SendMessageBatch`, health check, `AddAwsSqsEndPoint` / `WithAwsSqs()` (config type `AwsSqs`). Failed messages are retried through the queue's visibility timeout and redrive policy; messages that can never succeed go to the broker's dead-letter endpoint, because SQS cannot dead-letter a message on request. Bytes that SQS cannot carry as text are sent as Base64. Local testing with ElasticMQ: `scripts/setup-sqs.ps1`.
- `NymBroker.Endpoint.MySql`: a MySQL 8.0+ / MariaDB 10.6+ queue-table endpoint (#76) — same lifecycle, leases, retries and `Failed` rows as the PostgreSQL and SQL Server endpoints, `FOR UPDATE SKIP LOCKED` so several instances can poll one table, atomic `PostBatchAsync`, `AddMySqlEndPoint` / `WithMySql()` (config type `MySql`). Uses MySqlConnector.
- `NymBroker.Idempotency.MySql`: a durable idempotency store in a MySQL table (#76), `AddMySqlIdempotency`. Local testing: `scripts/setup-mysql.ps1`.
- `samples/NymBroker.AwsSqsSample`: SQS with the broker's dead-letter endpoint on a second queue, and FIFO ordering and deduplication.

## [0.10.0] - 2026-10-11

### Added

- `INymBroker.PostRawAsync(endpoint, Stream | ReadOnlyMemory<byte>, …)` posts bytes as-is: an existing envelope, or raw input for an input transformer (#74). A stream is read from its current position to the end, so non-seekable streams (a request body) work; the bytes are copied.
- `samples/NymBroker.EfCoreTransactionSample`: a consumer writing three tables with EF Core in one SQL Server transaction, with the SQL Server idempotency store.

### Removed

- **Breaking:** `INymBroker.PostAsync(string, Stream, …)` (#74). Passing a concrete stream such as `new MemoryStream(bytes)` bound to `PostAsync<T>` instead and failed at runtime; only a `(Stream)` cast reached it. Migration: `PostAsync(endpoint, (Stream)stream, …)` → `PostRawAsync(endpoint, stream, …)` — same parameters.

### Changed

- **Breaking:** `PostAsync<T>`, `PostBatchAsync<T>`, `PublishAsync<T>` and `PublishBatchAsync<T>` throw `ArgumentException` (pointing to `PostRawAsync`) when the message is a `Stream`, `byte[]`, `ReadOnlyMemory<byte>` or `Memory<byte>` (#74). Previously a stream failed with a serializer error, and bytes were posted as a Base64 message of type `System.Byte[]` that the receiver dropped as unknown.

## [0.9.5] - 2026-10-11

### Added

- RabbitMQ TLS: `RabbitMqSettings.UseTls`, `TlsServerName`, `ClientCertificatePath` and `ClientCertificatePassword`. The server certificate is always verified.
- [Production security](docs/security.md): TLS and certificate verification for RabbitMQ, SQL Server, PostgreSQL and Azure Service Bus, secrets, least-privilege permissions, and what can leak.

## [0.9.4] - 2026-10-11

### Added

- `samples/NymBroker.MediatRSample` and [MediatR patterns with NymBroker](docs/mediatr-comparison.md): commands, notifications, queries and pipeline behaviors as in MediatR, in a CQRS order API, with the reasoning and the pros and cons.

### Fixed

- RabbitMQ endpoint: stopping the listener now works properly (#73). `StopListeningAsync` stops new deliveries, waits for the message being handled, acks the pending batch (`BatchAckSize > 1`), then ends the listener. Previously the listener loop and the reconnect attempts kept running after stop, a message handled during stop and the unacked part of the batch were redelivered, handlers never saw the stop, and exceptions in the delivery callback were swallowed by RabbitMQ.Client. Starting an endpoint that is already listening now throws.
- Azure Service Bus endpoint: the processor is disposed when `StartProcessingAsync` fails (#73).

## [0.9.3] - 2026-10-10

### Added

- Building blocks for endpoint authors in `NymBroker` (Core), used by the built-in endpoints:
  - `LeasedQueueListener` (`NymBroker.Core.Endpoint.Queue`) — the listener loop of a lease-based queue table, with `ILeasedQueueSettings`, `QueueMessage`, `QueueMessageOutcome` and `QueueMessageStatus`. The SQLite, PostgreSQL and SQL Server endpoints now share it instead of three copies.
  - `EndpointHandler.InvokeAsync` — calls the broker's handler, logs an exception and returns `Retry`.
  - `HealthCheckResult.FromProbe` — runs an async connectivity probe with a timeout for `IEndPoint.HealthCheck()`.
  - `NymBrokerBuilder.AddConfiguredEndPoints` and `EndPointConfiguration.GetSettings<T>()` — the body of a `With…()` extension in one line.
  - `IExpiringIdempotencyStore` and `NymBrokerBuilder.AddIdempotencyStore` — registers a database idempotency store with a cleanup hosted service; the three `NymBroker.Idempotency.*` packages use it.
- `SqliteSettings`, `PostgresSettings` and `SqlServerSettings` implement `ILeasedQueueSettings`.

### Changed

- The SQLite endpoint writes the outcomes of handled messages on shutdown with a 10-second timeout (it waited without a limit), like the PostgreSQL and SQL Server endpoints.
- Log message templates unified across endpoints: a handler exception is logged as "Unhandled error dispatching message … on endpoint '…'" (RabbitMQ logged "Error processing message from {Queue}"), and a failed health check as "{EndpointKind} endpoint '{Name}' health check failed".

### Fixed

- `ApplyConfiguration` now sets `LoadedConfiguration`, so the documented `appsettings.json` route — `.ApplyConfiguration(BrokerConfigurationReader.Read(configuration)).WithSql()` (or any `With…()`) — registers those endpoints. Previously only `LoadConfiguration(file)` did, and the add-on entries were silently ignored.

- A consumer or subscriber that throws its own `OperationCanceledException` while the broker is not stopping — typically an `HttpClient` timeout (`TaskCanceledException`) — is now treated as a failure: retried by the transport or sent to the dead-letter endpoint, and counted as a failed consume. Previously the exception escaped `ProcessAsync` as if the broker were shutting down. The PostgreSQL and SQL Server endpoints then logged a misleading "Poll error" and left the message and the rest of its batch `InProgress` until the lease expired, and the Memory endpoint's listener stopped without logging anything. `ProcessAsync` still throws `OperationCanceledException` when its own token is cancelled.

## [0.9.2] - 2026-10-08

### Added

- Agent skills for building applications on NymBroker (`skills/`): setup, messages, consumers, routing, producer/worker, input transformers, scheduled actions, testing, observability, dead letters and troubleshooting. Download `nymbroker-skills.zip` from the GitHub release.
- Documentation: installing from NuGet, the samples and the benchmark.

### Changed

- The README's links are absolute, so they work on the nuget.org package page.

## [0.9.1] - 2026-10-07

### Changed

- `NymBroker` no longer depends on Cronos; cron expressions are parsed by a built-in, Cronos-compatible implementation.

