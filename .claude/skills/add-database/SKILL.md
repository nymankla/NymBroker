---
name: add-database
description: Add support for a new database to NymBroker as a complete family of four packages — queue-table endpoint, idempotency store, schedule store and outbox store — plus shared source, local Docker setup, tests, benchmark, docs and client skills. Use when the user asks to add, support or port NymBroker to a new database (e.g. Oracle, CockroachDB, Firebird), or asks for a schedule store or outbox store for a database, which have no skill of their own.
---

# Add a database

A database is supported through four packages. MySQL is the newest complete family and the one to copy:

| Package | Copy from | Procedure |
|---|---|---|
| `NymBroker.Endpoint.<Db>` | `NymBroker.Endpoint.MySql` | skill `write-endpoint` (lease-based queue, `LeasedQueueListener`) |
| `NymBroker.Idempotency.<Db>` | `NymBroker.Idempotency.MySql` | skill `write-idempotency-store` |
| `NymBroker.Scheduling.<Db>` | `NymBroker.Scheduling.MySql` | this skill |
| `NymBroker.Outbox.<Db>` | `NymBroker.Outbox.MySql` | this skill |

**Ask the user first**: four new projects and a new driver dependency are architectural decisions. Agree on the driver package, the oldest supported server version, the config type name (stable, for example `"MySql"`) and which of the four packages are wanted. No Dapper and no Windows-only driver.

## Order of work

Build and test one package at a time: endpoint, idempotency, scheduling, outbox. Each one must reach 0 warnings and green tests before the next.

### 1. Shared source and local infrastructure

- `shared/<Db>/`: code that all four packages need (connection handling, identifier quoting). Add a `SharedSource` line in `Directory.Build.targets` with `EndsWith('.<Db>')`. Add the new name to the `shared\Inspection` condition. If the driver pools connections like SQL Server and MySQL, add it to the `shared\Stores\Pooled` condition; otherwise create `shared/Stores/<Db>/`. Update the table in [shared/README.md](../../../shared/README.md). Everything in `shared/` stays `internal`.
- `scripts/docker-compose.yml`: a service with `pull_policy: always`, a health check and a data volume, pinned to a major version. Add `scripts/setup-<db>.ps1` with `-Stop` / `-Logs` (copy `setup-mysql.ps1`), a line in `scripts/CLAUDE.md`, and in `scripts/test-all.ps1` both the setup script and a `NYMBROKER_<DB>_CS` variable.

### 2. Endpoint and idempotency store

Follow `write-endpoint` and `write-idempotency-store`. In addition for a queue-table endpoint: implement `IQueueInspector` through `shared/Inspection` (`InspectCounts` must return the columns in the order pending, in progress, failed, completed, deferred) and write the folder `CLAUDE.md`.

### 3. Schedule store

Read `NymBroker.Core`'s `IScheduleStore` and `AddScheduleStore`, then copy the four files of `NymBroker.Scheduling.MySql` (`*ScheduleStore`, `*ScheduleSql`, `*ScheduleStoreSettings`, `NymBrokerBuilder*ScheduleExtensions`).

- One row for each action name (`name`, `last_run_utc`). No cleanup.
- The claim is one atomic statement that only moves `last_run_utc` forward. Two instances that claim the same run must give exactly one winner.
- Tests: `<Db>ScheduleStoreTests` derives from `ScheduleStoreContractTests` and runs only when `NYMBROKER_<DB>_CS` is set.

### 4. Outbox store

Read [docs/transactional-outbox.md](../../../docs/transactional-outbox.md), `shared/Outbox/OutboxStoreCore.cs` and `OutboxDialect.cs`, then copy `NymBroker.Outbox.MySql`. The package adds only its SQL and parameter types; the logic stays in `OutboxStoreCore`.

- The insert runs in the caller's `DbTransaction`. Never open a connection of your own for it.
- The claim takes rows with a lease and skips locked rows (`SKIP LOCKED` / `READPAST`). If the database has neither, stop and ask the user.
- Tests: `<Db>OutboxStoreTests` derives from `OutboxStoreContractTests`.

### 5. Wire in every place

Find the places with this search and add the new database to each:

```bash
rg -l -i "mysql" -g "!NymBroker.*.MySql/**" -g "!shared/MySql/**" -g "!CHANGELOG.md" -g "!**/bin/**" -g "!**/obj/**" .
```

- `NymBroker.slnx` and the project references in `NymBroker.Tests.csproj`; the cross-database tests (`LeasedQueueConcurrencyTests`, `RetentionTests`).
- Each `.csproj`: `PackageId`, `PackageDescription`, `PackageTags`, `InternalsVisibleTo` for `NymBroker.Tests`, the same driver version in all four.
- `PublicAPI.Shipped.txt` (only `#nullable enable`) and `PublicAPI.Unshipped.txt` in each project.
- `samples/NymBroker.Benchmarks` (project reference and scenario).
- `NymBroker.Dashboard/Ui/app.js` where it names the databases.
- `README.md` (absolute links only), `CONTRIBUTING.md`, root `CLAUDE.md` (endpoint list, solution layout, integration variable count), the `docs/` pages and the client `skills/*/SKILL.md` that list the databases.
- The symbol-package check in CI compares counts, so `ci.yml` needs no change. Check that `./scripts/pack.ps1` produces the new packages.

### 6. Finish

`./scripts/test-all.ps1` must pass with the new container. `CHANGELOG.md` → Added, one entry for each package. New packages are a minor version in 0.x; the level is the user's decision.
