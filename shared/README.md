# Shared source

Internal source files compiled into several packages (see `Directory.Build.targets`), so the packages share code without
a shared package or public API. Everything here must stay `internal`. The types have the same full name in each
assembly, so tests reach them through a forwarding member on the package's own SQL class.

| Folder | Compiled into |
|---|---|
| `Postgres`, `SqlServer`, `MySql` | every project for that database |
| `Stores/Postgres`, `Stores/Sqlite`, `Stores/Pooled` | the idempotency and schedule stores for that database (`Pooled`: SQL Server and MySQL) |
| `Idempotency` | every idempotency store |
