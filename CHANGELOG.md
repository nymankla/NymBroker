# Changelog

Notable changes to NymBroker are documented here. This changelog follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

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

