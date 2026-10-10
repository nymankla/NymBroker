# Samples

[← User guide](user-guide.md)

Runnable sample applications live in [`samples/`](../samples). Each one is a small console or web app that references the NymBroker projects in this repository directly, so you can run it from a clone without installing packages. In your own application, install the matching [NuGet packages](getting-started.md#requirements-and-packages) instead.

Run any sample from the repository root:

```bash
dotnet run --project samples/<SampleName>
```

## Overview

| Sample | Shows | Packages used | Needs |
|---|---|---|---|
| [NymBroker.Sample](../samples/NymBroker.Sample) | Memory + File endpoints, a consumer for two message types, a content-based route, interval and cron scheduled actions | `NymBroker` | — |
| [NymBroker.ConfigSample](../samples/NymBroker.ConfigSample) | Endpoints loaded from `queuesettings.json` with `LoadConfiguration`; routes and consumers in code | `NymBroker` | — |
| [NymBroker.CsvSample](../samples/NymBroker.CsvSample) | An input transformer that turns CSV lines into typed messages; invalid lines are dropped | `NymBroker` | — |
| [NymBroker.RoutingSample](../samples/NymBroker.RoutingSample) | Routing from an inbox to VIP / Standard queues, a topic with two subscribers and an endpoint subscription, source guards against routing loops | `NymBroker`, `NymBroker.Endpoint.Sqlite` | — (in-memory SQLite) |
| [NymBroker.SqlSample](../samples/NymBroker.SqlSample) | SQLite queue table: messages posted before start are claimed and dispatched once the broker starts | `NymBroker`, `NymBroker.Endpoint.Sqlite` | — (file `sqlsample.db`) |
| [NymBroker.MediatRSample](../samples/NymBroker.MediatRSample/README.md) | MediatR patterns (command, notification, query, pipeline behavior) with NymBroker in a CQRS order API; self-checking `--demo`, `--sqlite` for a durable command queue — see [MediatR patterns](mediatr-comparison.md) | `NymBroker`, `NymBroker.Endpoint.Sqlite` | — |
| [NymBroker.WebSample](../samples/NymBroker.WebSample) | ASP.NET Core minimal API that posts orders to a SQLite queue; OpenAPI + Scalar UI | `NymBroker`, `NymBroker.Endpoint.Sqlite` | — |
| [NymBroker.PostgresSample](../samples/NymBroker.PostgresSample) | Same flow as the SQLite sample on a PostgreSQL queue table | `NymBroker`, `NymBroker.Endpoint.Postgres` | `./scripts/setup-postgres.ps1` |
| [NymBroker.SqlServerSample](../samples/NymBroker.SqlServerSample) | Same flow on a SQL Server queue table | `NymBroker`, `NymBroker.Endpoint.SqlServer` | `./scripts/setup-sqlserver.ps1` |
| [NymBroker.RabbitSample](../samples/NymBroker.RabbitSample) | RabbitMQ producer and consumer in one process, or either role alone (`--mode producer\|consumer`, `--count N`) | `NymBroker`, `NymBroker.Endpoint.RabbitMq` | `./scripts/setup-rabbitmq.ps1` |
| [NymBroker.AzureServiceBusSample](../samples/NymBroker.AzureServiceBusSample) | A failing order is retried, dead-lettered by Service Bus, and read back from the dead-letter queue with its reason | `NymBroker`, `NymBroker.Endpoint.AzureServiceBus` | `./scripts/setup-servicebus.ps1` |
| [NymBroker.ProducerSample](../samples/NymBroker.ProducerSample/README.md) / [NymBroker.ConsumerSample](../samples/NymBroker.ConsumerSample/README.md) | Two separate processes sharing one queue: a `WriteOnly` producer that exits after posting, and a long-running worker (`--transport sqlite\|postgres\|rabbit`) | `NymBroker`, `NymBroker.Endpoint.Sqlite`, `.Postgres`, `.RabbitMq` | SQLite: — ; others: their setup script |
| [NymBroker.Benchmarks](../samples/NymBroker.Benchmarks) | Throughput and allocations per transport, batching and large-message splitting — see [Benchmarks](benchmarks.md) | all endpoint packages | Memory, File, SQLite always; Postgres, SQL Server, Service Bus, RabbitMQ when reachable on localhost |

## Local infrastructure

The samples that need a database or a message broker use Docker containers defined in [`scripts/docker-compose.yml`](../scripts/docker-compose.yml) (Docker Desktop required). Each setup script starts its service and waits until it is healthy; `-Stop` stops it and `-Logs` tails its log.

| Script | Service | Connection used by the samples |
|---|---|---|
| `./scripts/setup-postgres.ps1` | PostgreSQL | `Host=localhost;Database=nymbroker;Username=postgres;Password=postgres` |
| `./scripts/setup-sqlserver.ps1` | SQL Server 2022 | `Server=localhost,1433;Database=nymbroker;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True` |
| `./scripts/setup-rabbitmq.ps1` | RabbitMQ | `localhost:5672`, management UI on `http://localhost:15672` |
| `./scripts/setup-servicebus.ps1` | Azure Service Bus emulator | AMQP on `localhost:5673`; queues from `scripts/servicebus/Config.json` |

These credentials are for local development only.

## Where to start

- New to NymBroker: **NymBroker.Sample**, then **NymBroker.RoutingSample**.
- Choosing a durable queue: **NymBroker.SqlSample** (no setup), then the PostgreSQL, SQL Server, RabbitMQ or Service Bus sample for your transport.
- Separate producer and worker processes: **NymBroker.ProducerSample** with **NymBroker.ConsumerSample**.
- Dead-lettering and retries: **NymBroker.AzureServiceBusSample**, and [Reliability](reliability.md).
- Non-JSON input: **NymBroker.CsvSample**, and [Pipeline extensions](pipeline-extensions.md).
- Coming from MediatR, or building CQRS: **NymBroker.MediatRSample**, explained in [MediatR patterns with NymBroker](mediatr-comparison.md).
- Comparing transport and feature performance: **NymBroker.Benchmarks**, explained in [Benchmarks](benchmarks.md).
