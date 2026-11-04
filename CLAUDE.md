# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository. Details about one project live in a `CLAUDE.md` in that project's folder (see [Where the details are](#where-the-details-are)) and are loaded when you work there.

## What this is

A .NET 10 message-processing framework following Enterprise Integration Patterns, shipped as NuGet packages. Messages travel as JSON envelopes:

```
Source endpoint → deserialize → filter → route / topic / consumer → destination endpoint
                                       ↕ aggregator / splitter
```

Endpoints: Memory, File (in Core), SQLite, PostgreSQL, SQL Server, MySQL, RabbitMQ, Azure Service Bus, Amazon SQS, HTTP (send only) (one package each).

## Commands

Run from the solution root (`e:\utv\NymBroker`):

```bash
dotnet build                                                                  # must end with 0 warnings
dotnet test                                                                   # unit tests; integration tests are skipped
dotnet test --project NymBroker.Tests -- --filter-class "*SerializerTests"    # one class (Microsoft Testing Platform, see global.json)
./scripts/test-all.ps1                                                        # starts Docker containers, runs EVERY test incl. integration
./scripts/test-all.ps1 -Filter "*RabbitMqEndPointTests"                       # same, one class
./scripts/pack.ps1                                                            # packs all packages + artifacts/nymbroker-skills.zip
dotnet run -c Release --project samples/NymBroker.Benchmarks                  # throughput benchmark
```

Integration tests run only when their variable is set — `test-all.ps1` sets all six.

Local infrastructure (Docker Desktop) is in `scripts/docker-compose.yml` (every service has `pull_policy: always`, so each `up` pulls the newest image for its tag; tags stay on the current major version so the data volumes keep working), one script per service, each with `-Stop` / `-Logs`: `setup-rabbitmq.ps1` (5672, UI 15672), `setup-postgres.ps1` (5432), `setup-sqlserver.ps1` (1433, creates db `nymbroker`), `setup-mysql.ps1` (MySQL 8.4 on 3306, db `nymbroker`), `setup-servicebus.ps1` (emulator on 5673; starts SQL Server too, entities in `scripts/servicebus/Config.json`), `setup-sqs.ps1` (ElasticMQ, SQS API on 9324, UI 9325; queues are created by the tests). `setup-rabbitmq -Stop` / `setup-postgres -Stop` run `docker compose down` and stop **all** services; the SQL Server / MySQL / Service Bus / SQS `-Stop` stop only their own. If `docker` cannot connect, Docker Desktop is not running.

## Solution layout

| Project | Role |
|---|---|
| `NymBroker.Idempotency.Sqlite` / `.Postgres` / `.SqlServer` / `.MySql` | Durable `IExpiringIdempotencyStore`s (`AddSqliteIdempotency` …) registered through Core's `AddIdempotencyStore`; independent of the endpoint packages. SQLite is restart-safe on one host only |
| `NymBroker.Scheduling.Sqlite` / `.Postgres` / `.SqlServer` / `.MySql` | Durable `IScheduleStore`s (`AddSqliteScheduleStore` …) registered through Core's `AddScheduleStore`: named scheduled actions run once per cluster and catch up missed runs. One row per action name (`name`, `last_run_utc`), atomic forward-only claim; no cleanup |
| `shared/` | Internal source compiled into several packages through `Directory.Build.targets`, so they share code without a shared package or public API ([shared/README.md](shared/README.md)) |
| `NymBroker.Tests` | xUnit. Unit tests use Memory and SQLite `:memory:`; `PostgresEndPointTests`, `PostgresIdempotencyStoreTests`, `SqlServerEndPointTests`, `MySqlEndPointTests`, `MySqlIdempotencyStoreTests`, the `*ScheduleStoreTests` (on `ScheduleStoreContractTests`; SQLite always runs), `AzureServiceBusEndPointTests`, `RabbitMqEndPointTests` and `AwsSqsEndPointTests` need their `NYMBROKER_*` variable (their unit tests always run) |
| `samples/*` | Runnable apps — table in [docs/samples.md](docs/samples.md). Self-checking: `NymBroker.MediatRSample -- --demo`, `NymBroker.EfCoreTransactionSample` (needs SQL Server), `NymBroker.AwsSqsSample` (needs ElasticMQ), `NymBroker.RabbitSqlServerSample -- --demo` (needs RabbitMQ and SQL Server), `NymBroker.WebhookSample -- --demo` |
| `skills/` | Agent skills for **client** apps (`nymbroker-setup`, `-message`, `-consumer`, `-routing`, `-producer-worker`, `-input-transformer`, `-scheduled`, `-scheduled-recovery`, `-testing`, `-observability`, `-dead-letters`, `-troubleshoot`); zipped by `pack.ps1`, attached to each GitHub release. Skills for developing *this* repo are in `.claude/skills/` (`write-endpoint`, `write-idempotency-store`, `make-release`) |
| `docs/` | User documentation, index in [docs/user-guide.md](docs/user-guide.md) |

## Key abstractions

The broker starts a listener on every `IEndPointEventDriven` endpoint that isn't `WriteOnly` and wires it to `ProcessAsync`, which never throws (except for the broker's own cancellation) and returns how to settle the message:

| Situation | Source with `UsesNativeDeadLetter` (SQL tables, RabbitMQ, Service Bus) | Otherwise (Memory, File, SQS, opt-out, `PublishAsync`) |
|---|---|---|
| Handled, filtered, or no consumer | `Completed` | `Completed` |
| Undecodable / expired / unknown compression | `DeadLetter(reason)` | posted to the `WithDeadLetterEndpoint` endpoint, `Completed` (`Retry` if that post fails) |
| Consumer or topic subscriber throws | `Retry(ex)` — the transport redelivers, then dead-letters; `DeadLetter(ConsumerFailed)` for a consumer added with `RetryOnFailure = false` | posted to the dead-letter endpoint, `Completed` (`Retry` if that post fails) |
| A route's destination throws, or anything unexpected | `Retry(ex)` | `Retry(ex)` |

## Where the details are

| Working on | Read |
|---|---|
| Broker engine, envelope, routing, topics, consumers, idempotency, scheduling, splitter, health checks, configuration, Memory/File | [NymBroker.Core/CLAUDE.md](NymBroker.Core/CLAUDE.md) |
| A queue-table endpoint | `NymBroker.Endpoint.Sqlite/`, `.Postgres/`, `.SqlServer/`, `.MySql/` `CLAUDE.md` |
| HTTP | `NymBroker.Endpoint.Http/CLAUDE.md` |
| RabbitMQ / Service Bus / SQS | `NymBroker.Endpoint.RabbitMq/`, `.AzureServiceBus/`, `.AwsSqs/` `CLAUDE.md` |
| A new endpoint or idempotency store | `.claude/skills/write-endpoint`, `.claude/skills/write-idempotency-store`, [docs/writing-an-endpoint.md](docs/writing-an-endpoint.md) |
| User-facing behaviour | `docs/` — the docs describe the public contract; keep them in step with the code |
| Production configuration (TLS, secrets, permissions) | [docs/security.md](docs/security.md) |
| Release history and why things changed | [CHANGELOG.md](CHANGELOG.md), GitHub issues referenced as `#NN` |
| Making a release | `.claude/skills/make-release` |

## Key design rules

- **Ask before architectural decisions** (new projects, public API shape, new dependencies) — built incrementally with explicit sign-off.
- **Never swallow exceptions silently** — every catch block and fire-and-forget boundary logs. `OperationCanceledException` from the component's *own* stop token is a clean exit; one raised by user code (e.g. an HTTP timeout) is a failure.
- DI: `Microsoft.Extensions.DependencyInjection` only (no Autofac). Serialization: `System.Text.Json` only (no Newtonsoft). Retries: Core's `RetryPolicy` (no Polly). No Cronos in packable projects. No Dapper. No Windows-only endpoints.
- **Package naming**: transports `NymBroker.Endpoint.<Transport>`, idempotency stores `NymBroker.Idempotency.<Db>`, schedule stores `NymBroker.Scheduling.<Db>` (folder = project = package ID = namespace). Config type names (`"Sql"`, `"RabbitMq"`, …) are separate and stable.
- New receiving endpoints implement `IEndPointEventDriven`, call the handler through `EndpointHandler.InvokeAsync` and settle by its `ProcessResult`; a lease-based queue derives from `LeasedQueueListener`. `UsesNativeDeadLetter` only if the transport has its own dead-letter queue. Send-only sinks: plain `IEndPoint`, `Mode => WriteOnly`.
- Routes use `IRouteCondition` / `Func<JsonElement, bool>`; no XML/XSLT. `RouteContext` stays non-sealed with a virtual `Evaluate()`.
- Consumers are keyed services, key = `typeof(TConsumer).Name`.
- **`README.md` is also the NuGet readme**: every link in it must be absolute (`https://github.com/nymankla/NymBroker/blob/master/...`). Relative links are fine in `docs/`.
- **Versioning**: one `<Version>` in `Directory.Build.props` for all packages; the bump level is the user's call; never bump on an ordinary push. A breaking change needs a minor bump in 0.x. Public API is tracked by the PublicAPI analyzers (RS0016/RS0017 are errors).

## Definition of done

Before saying a change is finished:

- [ ] `dotnet build` — **0 warnings, 0 errors** (RS0026 and nullable warnings count).
- [ ] Tests added or updated for the behaviour; `dotnet test` passes. If an endpoint, idempotency store or the broker pipeline changed: `./scripts/test-all.ps1` passes too. For a bug fix, the new test fails without the fix.
- [ ] Public API changed → `PublicAPI.Unshipped.txt` of that project (removals as `*REMOVED*<line>`); never delete lines from `PublicAPI.Shipped.txt` by hand.
- [ ] `CHANGELOG.md` → entry under `[Unreleased]` (Added / Changed / Fixed / Removed; mark breaking changes **Breaking** with the migration).
- [ ] User-visible behaviour → `docs/` updated; public API or setup the client skills describe → `skills/*/SKILL.md` updated; the project's folder `CLAUDE.md` updated if its internals changed.
- [ ] Samples that use the changed API still build (they are in the solution) and, for self-checking samples, still pass.
- [ ] Commit message explains why; `Fixes #NN` when it closes an issue. After pushing to `master`, tell the user what the next release needs (changelog, PublicAPI, version level).
