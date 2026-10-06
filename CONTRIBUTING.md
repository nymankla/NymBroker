# Contributing to NymBroker

Thanks for considering a contribution. Issues and pull requests are welcome.

## Before you start

- For a bug or feature, search existing issues first. For substantial changes, open an issue to discuss the approach before investing in implementation.
- Keep changes focused and consistent with the existing project structure and conventions.
- For a new endpoint, read [Writing an endpoint](docs/writing-an-endpoint.md).

## Development

NymBroker targets .NET 10. From the repository root:

```bash
dotnet build
dotnet test
```

To run one test class:

```bash
dotnet test --project NymBroker.Tests -- --filter-class "*ClassName"
```

Tests that require PostgreSQL, SQL Server, or Azure Service Bus are skipped unless their corresponding `NYMBROKER_POSTGRES_CS`, `NYMBROKER_SQLSERVER_CS`, or `NYMBROKER_SERVICEBUS_CS` environment variable is configured. Most tests do not require external services.

Before submitting a pull request:

1. Add or update tests for behavior changes.
2. Run the relevant tests and `dotnet build`.
3. Update documentation when user-visible behavior or configuration changes.
4. Describe the change and any relevant testing in the pull request.

## API compatibility and releases

See [Versioning and API compatibility](docs/versioning-and-api-compatibility.md) for package versioning rules, release tags, API baseline workflow, and the breaking-change/deprecation policy.

## Pull requests

Keep each pull request focused. Explain the problem it solves, summarize the implementation, and include test results. Do not include generated build output, local configuration, or credentials.
