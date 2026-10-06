---
name: write-idempotency-store
description: Implement a durable, database-backed NymBroker IIdempotencyStore (idempotent receiver) in its own optional project, e.g. NymBroker.Idempotency.Postgres or NymBroker.Idempotency.Sqlite — settings, atomic claim SQL, complete/release, cleanup service, builder extension, tests and docs. Use when the user asks to add, write or create an idempotency store / idempotency database / duplicate-detection store for a database (PostgreSQL, SQLite, MySQL, Redis, ...), or works on issues #56 / #57.
---

# Write a NymBroker idempotency store

The reference implementation is **`NymBroker.Idempotency.SqlServer`** (#55). Copy its structure file for file; only the SQL and connection handling change per database. **Read these before writing code:**

- `NymBroker.Core/Idempotency/IIdempotencyStore.cs`: the contract.
- `NymBroker.Core/Impl/NymBrokerImpl.Idempotency.cs`: how the broker calls the store.
- `NymBroker.Idempotency.SqlServer/*.cs` and `NymBroker.Tests/SqlServerIdempotencyStoreTests.cs`.
- `docs/reliability.md#duplicate-detection`: the user-facing semantics.

## 1. The contract the store must keep

The interface is two-phase and async:

```csharp
ValueTask<IdempotencyClaimResult> TryClaimAsync(Guid messageId, CancellationToken ct); // Claimed | Duplicate | InProgress
ValueTask CompleteAsync(Guid messageId, CancellationToken ct); // handled: remember for the TTL
ValueTask ReleaseAsync(Guid messageId, CancellationToken ct);  // will be retried: forget an OPEN claim
```

| Situation when claiming | Result |
|---|---|
| No row, or the row has expired (TTL over, or lease abandoned) | `Claimed`, written as InProgress with `expires = now + lease` |
| Live row with status Completed | `Duplicate` |
| Live row with status InProgress | `InProgress` |
| The conflicting row vanished mid-claim | `InProgress`; the broker returns `Retry`, which is safe |

Rules every implementation must follow:

- **The claim is atomic across processes.** Two concurrent claims of one ID, from any number of instances, give exactly one `Claimed`. Use the database's own primitive, never read-then-write in application code:
  - SQL Server: `UPDATE … WITH (UPDLOCK, ROWLOCK) WHERE expired`, then `INSERT` and catch PK violations 2601/2627.
  - PostgreSQL / SQLite: `INSERT … ON CONFLICT (message_id) DO UPDATE SET … WHERE <expired> RETURNING`.
- **One `expires_at_utc` column** serves both states: the lease end while InProgress, the TTL end once Completed. "Live" means `expires_at_utc > now`, and cleanup is one range delete.
- **`CompleteAsync` must record the ID even if the row is gone** (the lease expired and cleanup ran): it is an upsert.
- **`ReleaseAsync` deletes only InProgress rows**, never Completed ones.
- **Use the database server's clock** (`SYSUTCDATETIME()`, `now()`, `unixepoch()`), not `DateTime.UtcNow`, so instances with skewed clocks agree.
- **Inline the status values as literals** (1 InProgress, 2 Completed), and quote and validate the table name. Never interpolate user values other than the validated, quoted identifier.
- **Let exceptions propagate.** The broker logs them and returns `Retry` (fail closed), so the store must not swallow or log them itself.
- **Make it thread-safe**, using pooled connections per operation. For SQLite, use one connection serialized by `SemaphoreSlim(1,1)`, as in `SqliteEndPoint`; `:memory:` needs a single persistent connection.

The broker, not the store, decides when to complete or release, skips `Guid.Empty`, and claims split messages only after reassembly. Don't duplicate any of that in the store.

## 2. Project layout (one project per database)

New project **`NymBroker.Idempotency.<Db>`**, namespace `NymBroker.Idempotency.<Db>`:

| File | Content |
|---|---|
| `NymBroker.Idempotency.<Db>.csproj` | Reference `NymBroker.Core` plus the driver, at the same version as the matching endpoint project. Add `InternalsVisibleTo NymBroker.Tests`. Do **not** reference the endpoint project (`NymBroker.Endpoint.<Db>`). |
| `<Db>IdempotencySettings.cs` | `ConnectionString`, `TableName`, `Ttl` (24 h), `LeaseTimeout` (5 min), `AutoCreateTable` (true), `CleanupInterval` (10 min; `Zero` disables it), `CleanupBatchSize` (1000), and `Validate()` throwing `ArgumentException`. Use whole seconds if the database's date arithmetic needs integers. |
| `<Db>IdempotencySql.cs` | Internal static SQL builders: `CreateSchema`, `Claim`, `Complete`, `Release`, `DeleteExpired`, plus identifier quoting. |
| `<Db>IdempotencyStore.cs` | Public `IIdempotencyStore`. Validate settings in the constructor, build the SQL once, create the schema lazily under a `SemaphoreSlim` with a `volatile bool` ready flag, and expose a public `DeleteExpiredAsync`. |
| `<Db>IdempotencyCleanupService.cs` | Internal `BackgroundService` with a `PeriodicTimer(CleanupInterval)` that calls `DeleteExpiredAsync`. Failures → `LogError` and continue; host-stop cancellation is a clean exit. |
| `NymBrokerBuilder<Db>IdempotencyExtensions.cs` | `Add<Db>Idempotency(this NymBrokerBuilder, settings?)`, described below. |

The extension method:

```csharp
var s = settings ?? new <Db>IdempotencySettings();
s.Validate();                                                  // fail at registration
builder.Services.RemoveAll<<Db>IdempotencySettings>();
builder.Services.RemoveAll<<Db>IdempotencyStore>();
builder.Services.AddSingleton(s);
builder.Services.AddSingleton(sp => new <Db>IdempotencyStore(s, sp.GetRequiredService<ILogger<<Db>IdempotencyStore>>()));
builder.AddIdempotentReceiver(sp => sp.GetRequiredService<<Db>IdempotencyStore>());   // Core wires it into the broker
if (s.CleanupInterval > TimeSpan.Zero)
    builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, <Db>IdempotencyCleanupService>());
```

Wire it in:
- Add the project to `NymBroker.slnx`, next to the other `NymBroker.*` projects.
- Add a `ProjectReference` from `NymBroker.Tests`.
- `scripts/pack.ps1` picks up every `NymBroker.*` folder automatically. Check that `.\scripts\pack.ps1` (without a bump flag) lists the new package.
- If you edit `.slnx` or `.csproj` files from Windows PowerShell 5.1, don't leave a UTF-8 BOM behind.

## 3. Schema

```
message_id      <uuid type>    PRIMARY KEY
status          small int      CHECK (status IN (1, 2))
expires_at_utc  timestamp      NOT NULL       -- index this for cleanup
created_at_utc  timestamp      NOT NULL DEFAULT now
```

`CreateSchema` must be idempotent: `IF NOT EXISTS` / `CREATE TABLE IF NOT EXISTS`, and the same for the index. Two stores on one table must both start cleanly.

## 4. Tests: `NymBroker.Tests/<Db>IdempotencyStoreTests.cs`

Copy `SqlServerIdempotencyStoreTests`.

**Gating:**
- Server databases are env-gated with the existing variable (`NYMBROKER_POSTGRES_CS`, …) via `Assert.SkipUnless`.
- Use a unique table per test instance (`nb_idem_<guid>`), dropped in `DisposeAsync`.
- SQLite with `:memory:` always runs.

**Always-run tests:**
- Settings validation, including at registration.
- Seconds rounding, if used.
- Table-name quoting.
- The extension registers the store as `IIdempotencyStore` plus the cleanup service; `CleanupInterval = Zero` registers no service.

**Database tests:**
- claim → InProgress → complete → Duplicate;
- release then reclaim, and release doesn't forget a completed row;
- complete without a row still records the ID;
- lease expiry takeover (1 s lease, wait 1.5 s);
- TTL expiry;
- `DeleteExpiredAsync` deletes only expired rows, in batches (`CleanupBatchSize = 2`);
- auto-create works across two stores;
- 16 concurrent claims across 4 store instances give exactly one `Claimed`, all others `InProgress`;
- **two brokers on one table**: the same message processed concurrently by both reaches a slow consumer exactly once, and a later redelivery is dropped.

Core's broker flow is already covered by `IdempotentReceiverTests` (in-memory store); don't re-test it per database.

## 5. Docs

- `docs/reliability.md` (Duplicate detection): add the database to the store list and remove the "planned" link.
- `README.md`: solution-layout row and the Idempotent Receiver section.
- `CLAUDE.md`: solution-layout row.
- `docs/pipeline-extensions.md`, if the extension list names the stores.

## 6. Done when

- [ ] `dotnet build` has 0 warnings, and every sample under `samples/` builds.
- [ ] `dotnet test` is green **with and without** the integration env vars. Start the database with `scripts/setup-<db>.ps1`.
- [ ] Run the new test class several times: the concurrency and expiry tests must not be flaky.
- [ ] Commit on a branch with `Fixes #<issue>`. Version bumps follow CLAUDE.md: patch on push, minor or major only if the user says so.
