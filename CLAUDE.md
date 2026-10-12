# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository. Details about one project live in a `CLAUDE.md` in that project's folder (see [Where the details are](#where-the-details-are)) and are loaded when you work there.

## What this is

A .NET 10 message-processing framework following Enterprise Integration Patterns, shipped as NuGet packages. Messages travel as JSON envelopes:

```
Source endpoint → deserialize → filter → route / topic / consumer → destination endpoint
                                       ↕ aggregator / splitter
```

Endpoints: Memory, File (in Core), SQLite, PostgreSQL, SQL Server, RabbitMQ, Azure Service Bus, Amazon SQS (one package each).

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

Integration tests run only when their variable is set — `test-all.ps1` sets all five:

```powershell
$env:NYMBROKER_POSTGRES_CS   = "Host=localhost;Database=nymbroker;Username=postgres;Password=postgres"
$env:NYMBROKER_SQLSERVER_CS  = "Server=localhost,1433;Database=nymbroker;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True"
$env:NYMBROKER_SERVICEBUS_CS = "Endpoint=sb://localhost:5673;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
$env:NYMBROKER_RABBITMQ_HOST = "localhost"
$env:NYMBROKER_SQS_URL       = "http://localhost:9324"
```

Local infrastructure (Docker Desktop) is in `scripts/docker-compose.yml`, one script per service, each with `-Stop` / `-Logs`: `setup-rabbitmq.ps1` (5672, UI 15672), `setup-postgres.ps1` (5432), `setup-sqlserver.ps1` (1433, creates db `nymbroker`), `setup-servicebus.ps1` (emulator on 5673; starts SQL Server too, entities in `scripts/servicebus/Config.json`), `setup-sqs.ps1` (ElasticMQ, SQS API on 9324, UI 9325; queues are created by the tests). `setup-rabbitmq -Stop` / `setup-postgres -Stop` run `docker compose down` and stop **all** services; the SQL Server / Service Bus / SQS `-Stop` stop only their own. If `docker` cannot connect, Docker Desktop is not running.

## Solution layout

| Project | Role |
|---|---|
| `NymBroker.Core` (package `NymBroker`) | Broker engine, serializer, routing, topics, splitter, idempotent receiver, scheduling, health checks, DI builder, Memory and File endpoints, `RetryPolicy`, and the shared endpoint building blocks |
| `NymBroker.Endpoint.Sqlite` / `.Postgres` / `.SqlServer` | Lease-based queue tables (`Add…EndPoint`, `WithSql` / `WithPostgres` / `WithSqlServer`) |
| `NymBroker.Endpoint.RabbitMq` / `.AzureServiceBus` / `.AwsSqs` | Broker transports (`AddRabbitMqEndPoint` / `WithRabbitMq`, `AddAzureServiceBusEndPoint` / `WithAzureServiceBus`, `AddAwsSqsEndPoint` / `WithAwsSqs`) |
| `NymBroker.Idempotency.Sqlite` / `.Postgres` / `.SqlServer` | Durable `IExpiringIdempotencyStore`s (`AddSqliteIdempotency` …) registered through Core's `AddIdempotencyStore`; independent of the endpoint packages. SQLite is restart-safe on one host only |
| `NymBroker.Tests` | xUnit. Unit tests use Memory and SQLite `:memory:`; `PostgresEndPointTests`, `PostgresIdempotencyStoreTests`, `SqlServerEndPointTests`, `AzureServiceBusEndPointTests`, `RabbitMqEndPointTests` and `AwsSqsEndPointTests` need their `NYMBROKER_*` variable (their unit tests always run) |
| `samples/*` | Runnable apps — table in [docs/samples.md](docs/samples.md). Self-checking: `NymBroker.MediatRSample -- --demo`, `NymBroker.EfCoreTransactionSample` (needs SQL Server), `NymBroker.AwsSqsSample` (needs ElasticMQ) |
| `skills/` | Agent skills for **client** apps (`nymbroker-setup`, `-message`, `-consumer`, `-routing`, `-producer-worker`, `-input-transformer`, `-scheduled`, `-testing`, `-observability`, `-dead-letters`, `-troubleshoot`); zipped by `pack.ps1`, attached to each GitHub release. Skills for developing *this* repo are in `.claude/skills/` (`write-endpoint`, `write-idempotency-store`) |
| `docs/` | User documentation, index in [docs/user-guide.md](docs/user-guide.md) |

## Key abstractions

```
IEndPoint                 ← every transport: Mode, PostAsync(byte[]), PostBatchAsync(IReadOnlyList<byte[]>), HealthCheck()
  IEndPointEventDriven    ← receiving: StartListeningAsync(handler) / StopListeningAsync; UsesNativeDeadLetter
EndpointMode              ← ReadWrite (default) | ReadOnly | WriteOnly (no listener)
ProcessResult             ← handler result the endpoint settles by: Completed | Retry(ex) | DeadLetter(reason, description)
INymBroker                ← PostAsync<T>, PostRawAsync (bytes as-is), PostBatchAsync<T>, PublishAsync<T>, Route<T>(), ProcessAsync, CheckHealthAsync
IConsume<T> / ISubscribe<T> ← one consumer per message type / any number of topic subscribers
IMessageContext           ← envelope metadata: Id, CorrelationId, Address, MessageType, Created, DeadLetter
LeasedQueueListener       ← shared poll loop of the queue-table endpoints;  EndpointHandler.InvokeAsync ← call a handler safely
```

The broker starts a listener on every `IEndPointEventDriven` endpoint that isn't `WriteOnly` and wires it to `ProcessAsync`, which never throws (except for the broker's own cancellation) and returns how to settle the message:

| Situation | Source with `UsesNativeDeadLetter` (SQL tables, RabbitMQ, Service Bus) | Otherwise (Memory, File, SQS, opt-out, `PublishAsync`) |
|---|---|---|
| Handled, filtered, or no consumer | `Completed` | `Completed` |
| Undecodable / expired / unknown compression | `DeadLetter(reason)` | posted to the `WithDeadLetterEndpoint` endpoint, `Completed` |
| Consumer or topic subscriber throws | `Retry(ex)` — the transport redelivers, then dead-letters | posted to the dead-letter endpoint, `Completed` |
| A route's destination throws, or anything unexpected | `Retry(ex)` | `Retry(ex)` |

## Where the details are

| Working on | Read |
|---|---|
| Broker engine, envelope, routing, topics, consumers, idempotency, scheduling, splitter, health checks, configuration, Memory/File | [NymBroker.Core/CLAUDE.md](NymBroker.Core/CLAUDE.md) |
| A queue-table endpoint | `NymBroker.Endpoint.Sqlite/`, `.Postgres/`, `.SqlServer/` `CLAUDE.md` |
| RabbitMQ / Service Bus / SQS | `NymBroker.Endpoint.RabbitMq/`, `.AzureServiceBus/`, `.AwsSqs/` `CLAUDE.md` |
| A new endpoint or idempotency store | `.claude/skills/write-endpoint`, `.claude/skills/write-idempotency-store`, [docs/writing-an-endpoint.md](docs/writing-an-endpoint.md) |
| User-facing behaviour | `docs/` — the docs describe the public contract; keep them in step with the code |
| Production configuration (TLS, secrets, permissions) | [docs/security.md](docs/security.md) |
| Release history and why things changed | [CHANGELOG.md](CHANGELOG.md), GitHub issues referenced as `#NN` |

## Key design rules

- **Ask before architectural decisions** (new projects, public API shape, new dependencies) — built incrementally with explicit sign-off.
- **Never swallow exceptions silently** — every catch block and fire-and-forget boundary logs. `OperationCanceledException` from the component's *own* stop token is a clean exit; one raised by user code (e.g. an HTTP timeout) is a failure.
- DI: `Microsoft.Extensions.DependencyInjection` only (no Autofac). Serialization: `System.Text.Json` only (no Newtonsoft). Retries: Core's `RetryPolicy` (no Polly). No Cronos in packable projects. No Dapper. No Windows-only endpoints.
- **Package naming**: transports `NymBroker.Endpoint.<Transport>`, stores `NymBroker.Idempotency.<Db>` (folder = project = package ID = namespace). Config type names (`"Sql"`, `"RabbitMq"`, …) are separate and stable.
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

## Release checklist

Only when the user asks for a release and has chosen the level:

1. `<Version>` in `Directory.Build.props` (edit the line directly; `pack.ps1 -Version` also works but drops blank lines — check the diff).
2. `CHANGELOG.md`: rename `[Unreleased]` content to `## [x.y.z] - YYYY-MM-DD`, keep an empty `[Unreleased]` above it.
3. Every `PublicAPI.Unshipped.txt`: move added lines to `PublicAPI.Shipped.txt`; for each `*REMOVED*<line>`, delete `<line>` from `Shipped`; leave `Unshipped` as `#nullable enable`. Build must stay at 0 warnings.
4. Commit "Release x.y.z", `git tag -a vx.y.z -m "NymBroker x.y.z"`, push `master`, then push the tag.
5. Watch the CI run (`gh run list --branch vx.y.z`, `gh run watch`): build, publish to nuget.org (trusted publishing), GitHub release with `nymbroker-skills.zip`. nuget.org lists the version a few minutes later.
