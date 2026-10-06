# NymBroker

NymBroker is a .NET 10 message-processing framework based on [Enterprise Integration Patterns](https://www.enterpriseintegrationpatterns.com/). It decouples producers from handlers: post typed messages to named endpoints, then filter, route, and dispatch them to consumers or subscribers. Start with the in-process Memory endpoint and add durable transports as needed.

## Packages and endpoints

Install `NymBroker` for the broker, Memory, and File endpoints. Each optional transport and durable idempotency store is a separate package.

| Package | Endpoint or capability |
|---|---|
| `NymBroker` | Memory and File |
| `NymBroker.Endpoint.Sqlite` | SQLite queue |
| `NymBroker.Endpoint.Postgres` | PostgreSQL queue |
| `NymBroker.Endpoint.SqlServer` | SQL Server queue |
| `NymBroker.Endpoint.RabbitMq` | RabbitMQ |
| `NymBroker.Endpoint.AzureServiceBus` | Azure Service Bus |
| `NymBroker.Idempotency.Sqlite` | Restart-safe idempotency on one host |
| `NymBroker.Idempotency.Postgres` | Shared durable idempotency |
| `NymBroker.Idempotency.SqlServer` | Shared durable idempotency |

## Quick start

Requires the [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0). Add the core and hosting packages to a new console app:

```bash
dotnet add package NymBroker
dotnet add package Microsoft.Extensions.Hosting
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

See [Getting started](docs/getting-started.md) for the complete runnable setup, message and consumer definitions, and hosting details.

## Documentation

The [user guide](docs/user-guide.md) links to detailed guides for:

- [Messages and consumers](docs/messages-and-consumers.md)
- [Sending and batching messages](docs/sending-messages.md)
- [Routing and publish/subscribe](docs/routing-and-pubsub.md)
- [Endpoints and configuration](docs/endpoints-and-configuration.md)
- [Reliability and idempotency](docs/reliability.md)
- [Retry policy](docs/resilience.md)
- [Pipeline extensions](docs/pipeline-extensions.md)
- [Metrics, tracing, and health checks](docs/observability.md)
- [Writing an endpoint](docs/writing-an-endpoint.md)

## Community and project policies

- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [Code of conduct](CODE_OF_CONDUCT.md)
- [Changelog](CHANGELOG.md)

## License

NymBroker is licensed under the [MIT License](LICENSE.txt).
