# NymBroker.Dashboard

Read-only live web dashboard, served from the app's ASP.NET Core pipeline. User docs: [docs/dashboard.md](../docs/dashboard.md). Issue #129.

## Shape

- **Public API** is only `NymBrokerDashboardExtensions` (`AddNymBrokerDashboard`, `MapNymBrokerDashboard` returning the `RouteGroupBuilder`) and `NymBrokerDashboardOptions` (validated in `AddNymBrokerDashboard`). Everything else is internal; the JSON shapes are not a public contract.
- **Routes** (`Api/DashboardEndpoints.cs`), all `GET`, all behind `DashboardAccess.FilterAsync`: `/` (index; redirects to a trailing slash because the page uses relative URLs), `/assets/{name}`, `/api/overview`, `/api/live` (SSE, `TypedResults.ServerSentEvents` of pre-serialized strings), `/api/health`, `/api/endpoints/{name}/messages`.
- **Trim/AOT**: `IsAotCompatible`, the request delegate generator (`EnableRequestDelegateGenerator`) and `DashboardJsonContext` (source-generated, string enums, camelCase). The generator cannot bind nullable reference properties through `[AsParameters]` (it emits `typeof(string?)`), so handlers take one `[AsParameters] DashboardContext` (services) and the messages route reads its query string by hand (`MessageQuery.TryRead`, which also gives the 400 texts).

## Data flow

- `Metrics/MetricCollector`: a `MeterListener` on the meters `NymBroker` and `NymBroker.Resilience`, started by the hosted `DashboardMetricsService` with the host. A measurement only adds to the current second (interlocked; `LogHistogram` for durations, a CAS max for message age, per-`source`-tag counters for at most 256 endpoints). `Roll` (once a second) moves that into fixed-size `Ring`s sized by `History`. Never allocate on the measurement path: it runs on the broker's hot path.
- `Api/SnapshotService`: one loop for all open pages, started by the first `Subscribe` and stopped when the last subscriber disposes. Each round: `Describe()`, `GetCountsAsync` per inspectable endpoint in parallel (5 s timeout each; an error is logged when an endpoint starts failing, not every round), recent rates, and a window of points that overlaps the previous snapshot (the page de-duplicates by second). Health runs beside it every `HealthInterval` (`RefreshHealthAsync` logs and never throws). Subscribers get a bounded(1) drop-oldest channel, so a slow page never queues snapshots. `GetCurrentAsync` serves the overview from the latest snapshot or collects one; collection is serialized by a semaphore.
- `Api/MessageViews`: envelope fields always; the payload only when `ShowPayloads ?? IsDevelopment()`, after `RedactPayload`, cut to `MaxPayloadBytes`, Base64 if not UTF-8.

## Security

`DashboardAccess`: allowed when the endpoint has `IAuthorizeData` metadata (an authorization policy on the group), or `AllowAnonymousRemoteAccess`, or the caller is loopback (IPv4-mapped too) **and** the request has no `X-Forwarded-For` / `Forwarded` header. One warning per process when there is no policy. Headers on every response: CSP (`default-src 'none'`, scripts/styles/connect only `'self'`), `X-Frame-Options: DENY`, `nosniff`, `Referrer-Policy: no-referrer`; API responses `no-store`, assets `no-cache` + ETag. Read-only on purpose: actions (replay, delete) wait for #115 and then need antiforgery and their own policy.

## UI (`Ui/`, embedded as `NymBroker.Dashboard.Ui.<file>`)

Plain ES module, no build step, no CDN. Build DOM with `h()` / `s()` and text nodes: **never `innerHTML`** with data (payloads are attacker-controlled), never a `style` attribute (CSP); set dynamic styles through the CSSOM (`el.style.x = …`, `setProperty`). Use `mount(root, …)`, not `root.append`, when children can be null. Hash routes: `#/`, `#/endpoints`, `#/endpoints/{name}`, `#/topology`, `#/health`. Each view returns `{ update, dispose }`; live snapshots call `update`. Colours are CSS variables for dark (default) and light (`data-theme`), and `prefers-reduced-motion` turns animations off. To see a change: run `samples/NymBroker.WebSample`, open `/nymbroker/`, `POST /orders/demo`.

## Tests

`NymBroker.Tests/Dashboard/`: `DashboardMetricsTests` (histogram, rolling, bounded memory, the real listener with a unique `source` tag), `DashboardUnitTests` (access rules on `DefaultHttpContext`, options, message views), `DashboardApiTests` (Kestrel on `127.0.0.1:0`, Memory + in-memory SQLite: page, assets/ETag, overview, messages incl. redaction, error codes, SSE, proxy refusal). The benchmark has `Memory – + dashboard` to measure the collector's cost.
