# NymBroker

[![NuGet](https://img.shields.io/nuget/v/NymBroker.svg)](https://www.nuget.org/packages/NymBroker/)

NymBroker is a .NET 10 message-processing framework based on [Enterprise Integration Patterns](https://www.enterpriseintegrationpatterns.com/). It decouples producers from handlers: post typed messages to named endpoints, then filter, route, and dispatch them to consumers or subscribers. Start with the in-process Memory endpoint and add durable transports as needed.

## Packages and endpoints

Install [`NymBroker`](https://www.nuget.org/packages/NymBroker/) from nuget.org for the broker, Memory, and File endpoints. Each optional transport and durable idempotency store is a separate package; all packages share one version.

| Package | Endpoint or capability |
|---|---|
| [`NymBroker`](https://www.nuget.org/packages/NymBroker/) | Memory and File |
| [`NymBroker.Endpoint.Sqlite`](https://www.nuget.org/packages/NymBroker.Endpoint.Sqlite/) | SQLite queue |
| [`NymBroker.Endpoint.Postgres`](https://www.nuget.org/packages/NymBroker.Endpoint.Postgres/) | PostgreSQL queue |
| [`NymBroker.Endpoint.SqlServer`](https://www.nuget.org/packages/NymBroker.Endpoint.SqlServer/) | SQL Server queue |
| [`NymBroker.Endpoint.RabbitMq`](https://www.nuget.org/packages/NymBroker.Endpoint.RabbitMq/) | RabbitMQ |
| [`NymBroker.Endpoint.AzureServiceBus`](https://www.nuget.org/packages/NymBroker.Endpoint.AzureServiceBus/) | Azure Service Bus |
| [`NymBroker.Idempotency.Sqlite`](https://www.nuget.org/packages/NymBroker.Idempotency.Sqlite/) | Restart-safe idempotency on one host |
| [`NymBroker.Idempotency.Postgres`](https://www.nuget.org/packages/NymBroker.Idempotency.Postgres/) | Shared durable idempotency |
| [`NymBroker.Idempotency.SqlServer`](https://www.nuget.org/packages/NymBroker.Idempotency.SqlServer/) | Shared durable idempotency |

## Quick start

Requires the [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0). Add the core and hosting packages to a new console app:

```bash
dotnet add package NymBroker
dotnet add package Microsoft.Extensions.Hosting
```

Add an optional package for each transport or idempotency store you use, for example:

```bash
dotnet add package NymBroker.Endpoint.Sqlite
```

In your host's service-registration callback, register a Memory endpoint and a consumer:

```csharp
services.AddNymBroker()
    .AddMemoryEndPoint("Orders")
    .AddConsumer<OrderConsumer>()
    .Build();
```

After starting the host and resolving `INymBroker` from dependency injection, post a message:

```csharp
await broker.PostAsync("Orders", new Order("ORD-1"));
```

See [Getting started](https://github.com/nymankla/NymBroker/blob/master/docs/getting-started.md) for the complete runnable setup, all package install commands, message and consumer definitions, and hosting details. The [samples](https://github.com/nymankla/NymBroker/blob/master/docs/samples.md) show each transport and feature in a runnable app.

**Using an AI coding agent?** Download the [NymBroker skills](https://github.com/nymankla/NymBroker/releases/latest/download/nymbroker-skills.zip) into your project's `.claude/skills/` folder. Your agent can then set NymBroker up (it asks which transport and options you want first), create messages, consumers and routes, write tests, add monitoring, and troubleshoot. [Install instructions](https://github.com/nymankla/NymBroker/blob/master/skills/README.md).

## Documentation

The [user guide](https://github.com/nymankla/NymBroker/blob/master/docs/user-guide.md) links to detailed guides for:

- [Messages and consumers](https://github.com/nymankla/NymBroker/blob/master/docs/messages-and-consumers.md)
- [Sending and batching messages](https://github.com/nymankla/NymBroker/blob/master/docs/sending-messages.md)
- [Routing and publish/subscribe](https://github.com/nymankla/NymBroker/blob/master/docs/routing-and-pubsub.md)
- [Endpoints and configuration](https://github.com/nymankla/NymBroker/blob/master/docs/endpoints-and-configuration.md)
- [Reliability and idempotency](https://github.com/nymankla/NymBroker/blob/master/docs/reliability.md)
- [Retry policy](https://github.com/nymankla/NymBroker/blob/master/docs/resilience.md)
- [Pipeline extensions](https://github.com/nymankla/NymBroker/blob/master/docs/pipeline-extensions.md)
- [Metrics, tracing, and health checks](https://github.com/nymankla/NymBroker/blob/master/docs/observability.md)
- [Writing an endpoint](https://github.com/nymankla/NymBroker/blob/master/docs/writing-an-endpoint.md)
- [Samples](https://github.com/nymankla/NymBroker/blob/master/docs/samples.md)
- [MediatR patterns and CQRS](https://github.com/nymankla/NymBroker/blob/master/docs/mediatr-comparison.md)
- [Benchmarks](https://github.com/nymankla/NymBroker/blob/master/docs/benchmarks.md)

## Community and project policies

- [Contributing](https://github.com/nymankla/NymBroker/blob/master/CONTRIBUTING.md)
- [Security policy](https://github.com/nymankla/NymBroker/blob/master/SECURITY.md)
- [Code of conduct](https://github.com/nymankla/NymBroker/blob/master/CODE_OF_CONDUCT.md)
- [Changelog](https://github.com/nymankla/NymBroker/blob/master/CHANGELOG.md)

## License

NymBroker is licensed under the [MIT License](https://github.com/nymankla/NymBroker/blob/master/LICENSE.txt).
