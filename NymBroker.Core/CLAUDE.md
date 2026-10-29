# NymBroker.Core

The `NymBroker` package: broker engine, serializer, routing, topics, consumers, idempotent receiver, scheduling, splitter, health checks, configuration, DI builder, Memory and File endpoints, `RetryPolicy`, and the building blocks for endpoint packages. No transport dependency.

## Configuration is builder-only

`NymBrokerImpl` is `internal`; `INymBroker` exposes only runtime operations. Routes (`AddRoute`, built and validated when added, registered with the internal `AddRoute(RouteContext)`), filters (`AddFilter(instance)` / `AddFilter<T>()`), scheduled actions, transformers, dead letter, wire tap and max age are collected by `NymBrokerBuilder` and applied in the `NymBrokerImpl` factory in `Build()`, before `StartAsync` validates them. Tests construct the broker with `TestBroker.Create` (InternalsVisibleTo `NymBroker.Tests`).

## Message envelope (wire format)

```json
{
  "id": "guid",
  "correlationId": "guid",
  "address": { "to": "FileOut", "from": "RabbitIn" },
  "messageType": "orders.created",
  "created": "2025-01-01T00:00:00Z",
  "message": { "...business payload..." }
}
```

- `messageType` = `[MessageName("short.name")]` if present, else the CLR `FullName`. `MessageTypeName.Get(type)` is the single resolver. `MessageTypeRegistry` (an `ImmutableDictionary`) resolves names → CLR types by `[MessageName]`, then `FullName`, then `AssemblyQualifiedName`. Types used in routes/consumers are registered at config time; `SplitMessage` and `UndecodableMessage` are pre-registered.
- An optional `deadLetter` object (`DeadLetterInfo`: `reason`, `description` capped at 4 096 chars, `exceptionType`, `sourceEndpoint`, `deadLetteredAt`, `deliveryCount`) is added only when a message is dead-lettered (replaced if dead-lettered again); normal envelopes are byte-for-byte unchanged. Exposed as `IMessageContext.DeadLetter`, survives routing. `DeadLetterEnvelope.Annotate(raw, info)` adds it to any bytes, preserving unknown properties; bytes that are not a JSON object become a new `nymbroker.undecodable` envelope.
- `RawMessageContext` is the internal deserialized form; it holds the payload as a `JsonElement` for deferred typing.

## Posting

- Every Post/Publish method creates its envelope through `NymBrokerImpl.NewContext`, which throws `ArgumentException` for a `Stream`, `byte[]`, `ReadOnlyMemory<byte>` or `Memory<byte>` message (pointing to `PostRawAsync`) — they would be serialized as a meaningless payload. Topic methods create the envelope before the topic lookup so the guard always runs.
- `PostRawAsync(endpoint, Stream | ReadOnlyMemory<byte>)` posts bytes as-is; a stream is copied from its current position (works for non-seekable streams), memory is copied with `ToArray` so callers can reuse buffers. The two overloads suppress RS0026 with a justification.
- `PostBatchAsync` / `PublishBatchAsync`: every message keeps its own envelope and is received and settled one by one; only the send is batched through `IEndPoint.PostBatchAsync` (default interface method = sequential `PostAsync`; overridden atomically by the SQL endpoints and per chunk by Service Bus). Named `PostBatchAsync`, not a `PostAsync` overload, because `PostAsync(endpoint, list)` would bind to `PostAsync<T>` with `T = List<…>`.
- `splitThresholdBytes` / `compress` apply per message (`NymBrokerImpl.ToWireMessages`, shared by all post paths). `PublishAsync<T>` rethrows a `Retry`'s exception (no transport to redeliver).

## Processing (`NymBrokerImpl.Processing.cs`)

`ProcessAsync(raw, source, ct)` never throws except `OperationCanceledException` from **its own** token; a consumer's own `OperationCanceledException` (e.g. an `HttpClient` timeout) is a failure. Results are in the root `CLAUDE.md` table. Helpers: `UsesNativeDeadLetter(source)`, `DeadLetterAsync(...)`, `TryPostToDeadLetterAsync` (posts `DeadLetterEnvelope.Annotate(raw, info)`; a failing post → `LogError`). For split messages the part that completes the group carries the reassembled message's result. The processing-duration histogram has a `result` tag (`completed`/`retry`/`dead_letter`); `nymbroker.messages.dead_lettered` is tagged `reason`/`source`/`mode` (`broker`|`native`).

`StartAsync` starts every `IEndPointEventDriven` endpoint whose `Mode` is not `WriteOnly`, and throws (`ValidateTarget`) if a route, topic, dead-letter or wire-tap target is not registered or is `ReadOnly`; routes and topics added after start are not checked. A startup exception → `LogError`, scheduled actions rolled back, rethrown.

## Output transformers

`AddOutputTransformer(transformer, endpoint)` (`_outputTransformers`, keyed by endpoint name). Every send to an endpoint goes through `SendAsync` / `SendBatchAsync` in `NymBrokerImpl.Processing.cs` (posts, routes, topics, wire tap, dead letter). Never call `endpoint.PostAsync` directly on a send path, or the transformer is bypassed. The helper deserializes the envelope to a `RawMessageContext` and sends the transformer's bytes; null skips the message, and non-envelope bytes throw. `ToWireMessages` throws for `splitThresholdBytes` on such an endpoint. `StartAsync` throws if the endpoint is unregistered or read-only, or if the broker listens on it without an input transformer.

## Routing

- `RouteContext` is non-sealed with a virtual `Evaluate()` — subclass for custom routing. Conditions implement `IRouteCondition`: `JsonRouteCondition` (`.When(Func<JsonElement,bool>)`), `FromRouteCondition` (`.WhenFrom`), `NotFromRouteCondition` (`.WhenNotFrom`), `MessageAgeRouteCondition` (`.WhenMessageIsOlderThan`), `AndRouteCondition`, `OrRouteCondition`.
- Predicates receive the **payload** element, not the envelope. `IAnyMessage` routes match every type.
- Chained content conditions (`When`, `WhenMessageIsOlderThan`, `And`, `Or`, topics' `When`) are AND-ed via `RouteConditions.Combine`, also with a `Condition` preset on a factory-created `RouteContext`. `WhenFrom` / `WhenNotFrom` are single fields.
- **Loop hazard**: routing to an event-driven endpoint the broker listens on re-evaluates all routes — infinite loop without `WhenFrom` / `WhenNotFrom`.

## Consumers and topics

- `ConsumerDispatcher` builds a compiled expression lambda per message type (cached in a `ConcurrentDictionary`; ~10× faster than `MethodInfo.Invoke`) and creates an async DI scope per dispatch, so scoped consumers (and a scoped `DbContext`) get one instance per message.
- A consumer may implement several `IConsume<T>`; `AddConsumer<T>()` registers all. **One consumer per message type**: a different second consumer throws at `AddConsumer` and in `ConsumerDispatcher.RegisterConsumer`; the same one twice is a no-op; two consumer classes with the same simple name throw (DI key collision). Several handlers → topic with `ISubscribe<T>` (`SubscriberDispatcher`).

## Idempotent receiver (`NymBrokerImpl.Idempotency.cs`)

`IIdempotencyStore` is two-phase: `TryClaimAsync` → `Claimed | Duplicate | InProgress`, then `CompleteAsync` (remember for the TTL) or `ReleaseAsync` (forget an open claim, never a completed one). A claim is a lease. `ProcessIdempotentlyAsync` wraps routing/topics/consumer (`RouteAndDispatchAsync`) after decoding, filters and split reassembly — split parts are never claimed. `Duplicate` → `Completed` + `nymbroker.messages.duplicates` (Debug log); `InProgress` → `Retry`; claim throws → `LogError` + `Retry` (fail closed); a `Retry` result or exception → release, anything else → complete, both with `CancellationToken.None` (failure → `LogError`, lease expires). `Guid.Empty` ids skip the check. Claiming *before* processing is deliberate: marking ids up front lost redelivered retries.

Registration: `AddIdempotentReceiver(ttl)` (in-memory), `(IIdempotencyStore)`, `<TStore>()`, `(Func<IServiceProvider, IIdempotencyStore>)`. Database packages call `AddIdempotencyStore<TStore>(factory, cleanupInterval, tableName)`; their store implements `IExpiringIdempotencyStore`, and Core adds the internal `IdempotencyCleanupService<TStore>` when the interval is positive (delete failure → `LogError`, retried next interval). One store per broker.

## Scheduled actions

Interval actions fire on a timer; cron actions use the internal `CronSchedule` (`NymBroker.Core.Scheduling`, Cronos-compatible, local time zone; Cronos is only a test oracle). Each runs in a `ScheduledActionHandle` (`IAsyncDisposable`). Public API is the builder only: `AddScheduledAction(TimeSpan | cron, Func<IServiceProvider, CancellationToken, Task>)` runs each occurrence in a new async DI scope (`RunScopedAsync`); inside, actions are `Func<CancellationToken, Task>` and each run is awaited. The internal broker still starts an action added after `StartAsync` at once (tests use it). `_scheduledActions` / `_activeScheduledActions` / `_scheduledActionsRunning` are guarded by `_scheduleLock` — a sync `Lock`, never held across an await, and not `_lifecycleLock`, so `AddScheduledAction` stays non-blocking and cannot deadlock against `StopAsync`. A throwing run → `LogError`, schedule continues; unexpected loop end → `LogCritical`; `DisposeAsync` logs instead of rethrowing, so `StopAsync` never throws because of an action. Cron and named actions share `StartTimedScheduledAction` over a `ScheduleTiming` (`CronTiming`, or `GridTiming` = multiples of the interval since the Unix epoch, whole ms); `DelayUntilAsync` waits in steps of at most `MaxScheduleDelay` (1 day) because `Task.Delay` rejects > ~49 days.

Named actions (`AddScheduledAction(name, interval | cron, action, configure?)`, RS0026 suppressed: the overloads differ in a required parameter): the builder validates the name (`ScheduledActionName`: ≤ 200 chars, no control chars, unique case-insensitively) and options; the broker's `AddNamedScheduledAction` claims each occurrence with `IScheduleStore.TryClaimAsync(name, utcOccurrence)` before running it (`RunClaimedOccurrenceAsync`). The claim counts as the run, whatever the outcome. Claim throws → `LogError`, occurrence skipped (never run unclaimed); lost claim → `LogDebug`. `MissedRunPolicy.RunOnce` → before the loop, `RunMissedOccurrenceAsync` reads `GetLastRunAsync` and runs `ScheduleTiming.FindMissed(lastRun, now, maxCatchUp)` (latest occurrence in (lastRun, now]) once; no recorded run → nothing to catch up; read failure → `LogError`, loop starts anyway. Store: `_scheduleStore`, default internal `InMemoryScheduleStore`, replaced by `SetScheduleStore` in the builder's factory when `AddScheduleStore(factory)` was called (the `NymBroker.Scheduling.*` packages do). Stores need no cleanup: one row per name.

## Splitter, aggregator, compression

- `SplitterImpl.Split(bytes, ISplitCondition)` makes `SplitMessage` parts (Base64 chunks, shared `CorrelationId`); `AggregatorImpl` reassembles by correlation id when `GroupSize` is reached; incomplete groups expire after 2 hours (purge count logged at Debug). Each `Aggregate` is lock-guarded with an `IsCompleted` flag so a second caller holding the same dictionary slot cannot reassemble or remove it again (TOCTOU guard).
- With `compress` (default true) the envelope is compressed by `ICompressor` (`BrotliCompressor`) before splitting **only if that makes it smaller** — offsets Base64's ~33 %; binary payloads fall back automatically. Parts carry `SplitMessage.Compression` (the codec name); an unknown codec → dead-lettered as `UnknownCompression`. If compression alone gets under the threshold it still ships as a one-part group, because compressed bytes are not valid JSON on their own.

## Health check (`NymBrokerImpl.HealthCheck.cs`)

`CheckHealthAsync` runs every endpoint's `HealthCheckAsync` in parallel (wrapped in `Task.Run`, so the default implementation — the synchronous `HealthCheck()` — cannot block the caller), waits up to `BrokerHealthCheckOptions.Timeout` (10 s; a late endpoint → `Unhealthy` "timed out", its task left running), never throws except caller cancellation. Broker not started or a critical endpoint unhealthy → `Unhealthy`; only non-critical ones → `Degraded`. `.ConfigureHealthCheck(o => o.NonCritical("Audit"))` (`Build()` throws for unknown names). `services.AddHealthChecks().AddNymBroker(...)` registers `NymBrokerHealthCheck`. Endpoints that contact a server override `HealthCheckAsync` with `HealthCheckResult.FromProbeAsync(kind, name, logger, probe, cancellationToken: ct)` (5 s timeout; caller cancellation propagates) and implement `HealthCheck()` by waiting for it. Metrics: `nymbroker.health.checks`, `nymbroker.health.endpoint.failures`, gauges `nymbroker.health.status`, `nymbroker.health.endpoint.healthy`.

## Configuration and DI

- Config section `NymBroker` → `Endpoints[]` with `Name`, `Type`, `Config` (camelCase settings). `Type` is an **open string**: `EndPointType` holds the built-in constants (`EndPointType.BuiltIn`); unknown types load without error and are picked up by their package's `With*()` via `ep.IsType(name)` (case-insensitive). A new transport defines its type constant in its own package — Core does not change. `File` and `Memory` are processed by `LoadConfiguration` itself.
- `NymBrokerBuilder.Services` and `LoadedConfiguration` are public for extension packages; both `LoadConfiguration(file)` and `ApplyConfiguration(config)` set `LoadedConfiguration`. A `With…()` body is one line: `builder.AddConfiguredEndPoints(type, ep => builder.Add…(ep.Name, ep.GetSettings<TSettings>(), ep.Mode))`.

## Shared endpoint building blocks

- `EndpointHandler.InvokeAsync` — calls the handler; an exception → `LogError` + `Retry`; only the stop token's cancellation is rethrown.
- `AsyncOnce` (`Endpoint`) — runs an async initialization once (schema creation in the SQL endpoints and idempotency stores); concurrent callers wait, a failure is retried by the next caller, and the call after success is lock- and allocation-free when given a `static` lambda plus state.
- `LeasedQueueListener` (`Endpoint/Queue`) — owns start/stop, the drain/idle loop, the `ProcessResult` → `QueueMessageStatus` mapping (Completed; DeadLetter → Failed; Retry → Pending, or Failed at `MaxRetryCount`) and its log lines, poll-error / `Critical` logging, and the shutdown write with a fresh 10 s token. Subclasses supply `FinalizeAndClaimAsync` (write outcomes + claim next batch in one transaction) and `FinalizeAsync`; optional `WaitWhenIdleAsync` / `OnLoopExitAsync`. Settings implement `ILeasedQueueSettings` (an interface so no shipped settings members moved).

## Memory and File endpoints

- `MemoryQueueEndPoint`: bounded `Channel<byte[]>` (backpressure); optional `ILogger` (defaults to `NullLogger`, so `new MemoryQueueEndPoint("x")` works in tests). A failing or non-`Completed` message → `LogError`, no redelivery; unexpected loop end → `LogCritical`. Non-interface `ReadAsync` drains what's there (tests and samples).
- `FileEndPoint`: `ReadAndArchiveAsync` opens with `FileShare.ReadWrite | FileShare.Delete` — `Delete` is required so `File.Move` can rename while the read handle is open (Windows sharing violation otherwise). IOException retried via `RetryPolicy`, then `LogWarning` and skip. `OnFileCreated` failures / non-`Completed` → `LogError` (file already renamed, no retry). Existing files at start: per-file failures logged, the rest still processed.

## Retry policy (`Resilience/`)

`RetryPolicy(new RetryOptions { … })`, one per endpoint, `ExecuteAsync(token => …, ct)`; constant/exponential backoff with optional jitter; default `ShouldHandle` retries everything except `OperationCanceledException`; `OnRetry` should log. Meter name `RetryPolicy.InstrumentationName` stays `"NymBroker.Resilience"` for compatibility. Options: [docs/resilience.md](../docs/resilience.md).

## Performance notes

`RecyclableMemoryStream` for serialization; `MessageSerializerJson.PropCache` (one reflection lookup per `MessageContext<T>` type); routes, filters and consumer keys in immutable collections, written at config time with `ImmutableInterlocked.Update`.

## Metrics

Meter `NymBroker`: `nymbroker.messages.received`, `.routed`, `.consumed`, `.failed`, `.dead_lettered`, `.duplicates`, `nymbroker.message.processing.duration`, `nymbroker.message.age` (s, now − envelope `Created`, recorded before the TTL check; skipped when `Created` is unset, clamped at 0), plus the health metrics above. Names are public contract ([docs/observability.md](../docs/observability.md)).
