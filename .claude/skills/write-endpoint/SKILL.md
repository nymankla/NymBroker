---
name: write-endpoint
description: Implement a new NymBroker transport endpoint (IEndPoint / IEndPointEventDriven) — sink, push listener, or pull/poll queue — plus its builder extension, optional config loading, and tests. Use when the user asks to add, write, create or scaffold an endpoint, transport, connector or adapter for NymBroker (e.g. "add a Kafka endpoint", "write an SQS endpoint", "new transport for Redis"), or asks how endpoints work.
---

# Write a NymBroker endpoint

The full reference, with three compiled samples, is [docs/writing-an-endpoint.md](../../../docs/writing-an-endpoint.md). **Read it before writing code.** This skill is the procedure; the guide has the code to copy.

## 1. Pin down the transport (ask if unclear)

Answer these before writing code. Ask the user only for the ones the request and the transport's docs don't settle:

- **Direction:** send-only, receive-only, or both? A send-only endpoint is a *sink*.
- **Receive model:** does the transport *push* (callback, socket, consumer subscription) or must you *pull* (poll, receive-batch)?
- **Ack model:** can a message be acked/nacked, abandoned or leased? Is there a max-delivery-count or dead-letter feature?
- **Size limit** per message (matters for `splitThresholdBytes`).
- **Client library** and whether its client is thread-safe.
- **Where it lives:** a new `NymBroker.Endpoint.<Transport>` project (namespace `NymBroker.Endpoint.<Transport>`) in this repo (default for a reusable transport), or code in an app/sample.

Per CLAUDE.md, a new project is an architectural decision: **confirm with the user before creating one.**

## 2. Pick the shape

| Shape | Implement | Copy from |
|---|---|---|
| Sink | `IEndPoint`, `Mode => EndpointMode.WriteOnly` | Guide §3 (`ConsoleSinkEndPoint`) |
| Push | `IEndPointEventDriven` | Guide §4 (`UdpEndPoint`); in repo: `MemoryQueueEndPoint`, `RabbitMqEndPoint` |
| Pull | `IEndPointEventDriven` + internal poll loop | Guide §5 (`BrokeredQueueEndPoint`); in repo: `PostgresEndPoint`, `SqliteEndPoint` |

There is no `IEndPointPoll`; a pull transport still implements `IEndPointEventDriven`.

## 3. Implement

Start from the matching sample in the guide. Then verify every item in the guide's §9 checklist. These are the ones that get missed most often:

- `StartListeningAsync` **returns immediately**. The loop runs in `Task.Run(..., CancellationToken.None)` on a linked CTS; `StopListeningAsync` cancels and **awaits** the loop.
- Settle every message by the handler's `ProcessResult` (guide §1): `Completed` → ack; `Retry` → nack/abandon so it is redelivered (or log it as lost if the transport can't redeliver); `DeadLetter` → the transport's dead-letter queue with `Reason`/`Description`. A handler exception counts as `Retry`. **The loop continues** either way.
- If the endpoint can read the transport's own dead-letter queue, pass each received body through `DeadLetterEnvelope.Annotate(body, new DeadLetterInfo(...))` before calling the handler, so consumers see a uniform `context.DeadLetter`.
- `UsesNativeDeadLetter => _settings.UseNativeDeadLetter` (default `true`) **only** if the transport has its own dead-letter queue; otherwise leave the interface default (`false`) and the broker's dead-letter endpoint takes failures.
- Three catch layers: per message (`when (ex is not OperationCanceledException)`) → `LogError`; `OperationCanceledException` → swallow; anything else → `LogCritical`. Never a silent catch.
- `PostAsync` sends the bytes unchanged and is safe to call concurrently.
- **Pull loops drain back to back**: wait `PollInterval` only after an empty poll. For a database-backed queue, claim a batch in one statement, write batch N's outcomes in the same transaction as batch N+1's claim (guarded by the claimed attempt number), and prepare statements once. Copy the shape of `SqliteEndPoint` / `PostgresEndPoint` (guide §5, "Drain back to back" and "batch the round trips").
- Override `PostBatchAsync(IReadOnlyList<byte[]>)` when the transport can batch (one statement / transaction / client batch); keep order and one envelope per message, and document atomicity. Otherwise rely on the default (sequential `PostAsync`).
- `HealthCheck()` has a timeout and never throws.
- Retries use `NymBroker.Core.Resilience.RetryPolicy` (one per endpoint, `OnRetry` logs; options in `docs/resilience.md`). **Never add Polly.**
- Constructor: `(string name, TSettings settings, ILogger<T> logger, EndpointMode mode = EndpointMode.ReadWrite)`.
- Use `RetryPolicy` (`NymBroker.Core.Resilience`) if applicable
- Always try to use best practices when implementing the endpoint. If implementing a new transport, follow the patterns established in the existing ones. 
- Optimize for performance and reliability, using best practices for underlying transport and infrastructure.
- Ensure proper disposal of resources and handle exceptions gracefully to maintain system stability.
- Follow consistent naming conventions and code style to improve readability and maintainability.

For a new project, mirror `NymBroker.Endpoint.RabbitMq/`. The folder, project file, `PackageId` and namespace are all `NymBroker.Endpoint.<Transport>`. The project has a csproj that references only `NymBroker.Core` plus the client package, with `PackageId`/`PackageDescription` set; `<Transport>Settings.cs`; `<Transport>EndPoint.cs`; `NymBrokerBuilder<Transport>Extensions.cs`. Add the project to `NymBroker.slnx`; `scripts/pack.ps1` picks up `NymBroker.*` projects automatically.

## 4. Register

The builder extension must make **both** calls (guide §6):

```csharp
builder.Services.AddKeyedSingleton<IEndPoint>(name, (sp, _) => new XEndPoint(name, s, sp.GetRequiredService<ILogger<XEndPoint>>(), mode));
builder.RegisterEndpoint(name);
```

Config-file support: add a `WithX()` extension that matches entries with `ep.IsType(XEndPointType.X)`, using a type-name constant defined in your own package (guide §7). `Type` is an open string, so Core needs no change; don't add your type to `EndPointType`.

## 5. Test

Add `NymBroker.Tests/<Transport>EndPointTests.cs` and reference the project from `NymBroker.Tests.csproj`. The default run needs no external infrastructure: use a fake client, loopback, `:memory:` or a temp file.

Tests against a real server are **env-gated**, like `PostgresEndPointTests`:
- `NYMBROKER_<TRANSPORT>_CS` plus `Assert.SkipUnless(...)`, so the tests are skipped, not failed, when it's unset;
- a unique table or queue per test, cleaned up in `DisposeAsync`;
- a `scripts/setup-<transport>.ps1` and a docker-compose service to start the server locally.

Make all tests parallel-safe with unique names, ports and tables. Cover:

1. A posted message reaches the handler with the bytes unchanged.
2. Each `ProcessResult` settles correctly (`Completed` → ack, `Retry` → redelivered, `DeadLetter` → dead-lettered with the reason); a throwing handler counts as `Retry`; the next message is still processed.
3. A poll/receive error is logged and the loop recovers (pull shape).
4. After `StopListeningAsync` returns, the handler is never called again.
5. `HealthCheck()` reports a failed client as unhealthy without throwing.

Then run `dotnet build` (must have 0 warnings) and `dotnet test`, both without the env var and, if you added integration tests, with it set. Also build every sample, since some are excluded from the solution's Debug build.

## 6. Document

- Add the project to the CLAUDE.md solution table, and add its rows to the "Error Handling / Logging Guarantees" table.
- Add a usage snippet to the CLAUDE.md "Factory / DI" section and to `README.md` (its Endpoints section and its "Solution layout" table).
- Add the package to the package table in `docs/getting-started.md`, and a section for it in `docs/endpoints-and-configuration.md`.
- If the endpoint revealed something the guide doesn't cover, update `docs/writing-an-endpoint.md` too.
