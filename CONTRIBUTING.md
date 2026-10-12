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

Tests that require PostgreSQL, SQL Server, Azure Service Bus or RabbitMQ are skipped unless `NYMBROKER_POSTGRES_CS`, `NYMBROKER_SQLSERVER_CS`, `NYMBROKER_SERVICEBUS_CS` or `NYMBROKER_RABBITMQ_HOST` is set. Most tests need no external services. To run everything, with Docker Desktop installed:

```powershell
./scripts/test-all.ps1                                    # starts the containers, sets the variables, runs all tests
./scripts/test-all.ps1 -Filter "*PostgresEndPointTests"   # one class
./scripts/test-all.ps1 -SkipInfra                         # containers already running
```

Before submitting a pull request:

1. Add or update tests for behavior changes. For a bug fix, the test should fail without the fix.
2. `dotnet build` with no warnings, and `dotnet test`. If you changed an endpoint, an idempotency store or the broker pipeline, also run `./scripts/test-all.ps1`.
3. If the public API changed, update the project's `PublicAPI.Unshipped.txt` (the build fails otherwise; removals as `*REMOVED*<line>`).
4. Add an entry under `[Unreleased]` in `CHANGELOG.md`; mark breaking changes and describe the migration.
5. Update `docs/` when user-visible behavior or configuration changes, and `skills/` when the API or setup they describe changes.
6. Describe the change and how you tested it in the pull request.

## API compatibility and releases

See [Versioning and API compatibility](docs/versioning-and-api-compatibility.md) for package versioning rules, release tags, API baseline workflow, and the breaking-change/deprecation policy.

## Pull requests

Keep each pull request focused. Explain the problem it solves, summarize the implementation, and include test results. Do not include generated build output, local configuration, or credentials.
