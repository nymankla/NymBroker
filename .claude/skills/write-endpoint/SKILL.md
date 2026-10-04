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
- **Where it lives:** a new `NymBroker.<Transport>` project in this repo (default for a reusable transport), or code in an app/sample.

Per CLAUDE.md, a new project or a change to `EndPointType` is an architectural decision: **confirm with the user before creating either.**

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
- Handler failure: `LogError`, then nack/abandon (or drop if the transport can't redeliver), and **the loop continues**. Ack only after the handler succeeds.
- Three catch layers: per message (`when (ex is not OperationCanceledException)`) → `LogError`; `OperationCanceledException` → swallow; anything else → `LogCritical`. Never a silent catch.
- `PostAsync` sends the bytes unchanged and is safe to call concurrently.
- `HealthCheck()` has a timeout and never throws.
- Retries use `NymBroker.Resilience.RetryPolicy` (one per endpoint, `OnRetry` logs). **Never add Polly.**
- Constructor: `(string name, TSettings settings, ILogger<T> logger, EndpointMode mode = EndpointMode.ReadWrite)`.

For a new project, mirror `NymBroker.RabbitMq/`: a csproj that references only `NymBroker.Core` plus the client package, with `PackageId`/`PackageDescription` set; `<Transport>Settings.cs`; `<Transport>EndPoint.cs`; `NymBrokerBuilder<Transport>Extensions.cs`. Add the project to the solution and to `scripts/pack.ps1`.

## 4. Register

The builder extension must make **both** calls (guide §6):

```csharp
builder.Services.AddKeyedSingleton<IEndPoint>(name, (sp, _) => new XEndPoint(name, s, sp.GetRequiredService<ILogger<XEndPoint>>(), mode));
builder.RegisterEndpoint(name);
```

Config-file support (`WithX()`) also needs a new `EndPointType` member in `NymBroker.Core` (guide §7). That's a core change, so ask first.

## 5. Test

Add `NymBroker.Tests/<Transport>EndPointTests.cs`. No external infrastructure: use a fake client, loopback or `:memory:`. Make tests parallel-safe with unique names, ports and tables. Cover:

1. A posted message reaches the handler with the bytes unchanged.
2. A throwing handler leads to nack/abandon, and the next message is still processed.
3. A poll/receive error is logged and the loop recovers (pull shape).
4. After `StopListeningAsync` returns, the handler is never called again.
5. `HealthCheck()` reports a failed client as unhealthy without throwing.

Then run `dotnet build` (must have 0 warnings) and `dotnet test`.

## 6. Document

- Add the project to the CLAUDE.md solution table, and add its rows to the "Error Handling / Logging Guarantees" table.
- Add a usage snippet to the CLAUDE.md "Factory / DI" section and to `README.md`.
- If the endpoint revealed something the guide doesn't cover, update `docs/writing-an-endpoint.md` too.
