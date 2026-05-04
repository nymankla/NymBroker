# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A .NET 10 enterprise message processing framework following EIP (Enterprise Integration Patterns). Messages flow as JSON envelopes through a configurable pipeline:

```
Source Endpoint → Deserialize → Filter → Router → Consumer / Destination Endpoint
                                              ↕
                                    Aggregator / Splitter
```

Primary endpoints are **RabbitMQ** and **File**. Memory endpoint is used for in-process routing and tests.

## Commands

Run from the solution root (`e:\utv\POC\MessageBroker`):

```bash
dotnet build
dotnet test
dotnet test --filter "FullyQualifiedName~SerializerTests"   # single test class
dotnet run --project MessageBroker.Sample                    # run the sample

# RabbitMQ (Docker Desktop required)
./setup-rabbitmq.ps1          # start + wait for healthy
./setup-rabbitmq.ps1 -Stop    # stop
./setup-rabbitmq.ps1 -Logs    # tail logs
```

## Architecture

### Solution Layout

| Project | Role |
|---|---|
| `MessageBroker.Core` | Framework core — endpoints, serializer, routing, broker engine, factory. No RabbitMQ dependency. |
| `MessageBroker.RabbitMq` | Optional add-on — `RabbitMqEndPoint`, `RabbitMqSettings`, and `AddRabbitMqEndPoint`/`WithRabbitMq` extension methods. Reference only when using RabbitMQ. |
| `MessageBroker.Tests` | xUnit tests (use Memory endpoint — no RabbitMQ/file I/O) |
| `MessageBroker.Sample` | Runnable demo with file + memory endpoints |

### Key Abstractions

```
IEndPoint                 ← all transports
  IEndPointPoll           ← pull-based: ReadAsync yields current items then completes
  IEndPointEventDriven    ← push-based: StartListeningAsync + handler callback

IMessageContext<T>        ← typed envelope (Id, CorrelationId, Address, MessageType, Created, TraceParent...)
RawMessageContext          ← internal deserialized form; holds JsonElement RawMessage for deferred typing

IMessageBroker            ← engine facade (see below)
IConsume<T>               ← consumer contract; implement + register via AddConsumer<T>()
IRouteBuilder<T>          ← fluent route definition (see Routing section)
IRouteCondition           ← composable predicate evaluated on (IMessageContext, JsonElement)
```

### Message Envelope (JSON wire format)

```json
{
  "id": "guid",
  "correlationId": "guid",
  "address": { "to": "FileOut", "from": "RabbitIn" },
  "messageType": "orders.created",
  "created": "2025-01-01T00:00:00Z",
  "traceParent": "00-...",
  "message": { "...business payload..." }
}
```

`messageType` is resolved from `[MessageName("short.name")]` if present, otherwise the CLR `FullName`. `MessageTypeName.Get(type)` is the single resolver.

### IMessageBroker API

```csharp
broker.PostAsync<T>(endpoint, message)              // serialize + send
broker.PostAsync(endpoint, stream)                  // send pre-serialized

// Routing (all return IRouteBuilder<T> or RouteContext)
broker.Route<Order>()...Build()                     // typed route
broker.Route()...Build()                            // IAnyMessage route
broker.Route(IRouteBuilder)                         // custom builder
broker.Route(Func<RouteContext>)                    // custom factory

broker.AddFilter(IMessageFilter)
broker.AddScheduledAction(TimeSpan, Action)
broker.AddScheduledAction<T1>(TimeSpan, Action<T1>, T1)
broker.AddScheduledAction<T1,T2>(TimeSpan, Action<T1,T2>, T1, T2)
broker.AddScheduledAction<T1>(string cronExpr, Action<T1>, T1)   // Cronos cron syntax

broker.StartAsync(ct) / StopAsync(ct)              // called automatically by IHostedService
broker.ProcessAsync(raw, sourceEndpoint, ct)        // entry point for endpoint listeners
```

### Routing

`RouteContext` is an open (non-sealed) class with a `virtual Evaluate()` — subclass it for custom routing logic. Route conditions implement `IRouteCondition`:

| Condition class | Created by |
|---|---|
| `JsonRouteCondition` | `.When(Func<JsonElement, bool>)` |
| `FromRouteCondition` | `.WhenFrom(name)` |
| `NotFromRouteCondition` | `.WhenNotFrom(name)` |
| `MessageAgeRouteCondition` | `.WhenMessageIsOlderThan(TimeSpan)` |
| `AndRouteCondition` | `.And(lhs, rhs)` |
| `OrRouteCondition` | `.Or(lhs, rhs)` |

Predicates receive the **message payload** element (not the full envelope). `IAnyMessage` routes match every type.

```csharp
broker.Route<Order>()
    .To("FileOut")
    .WhenFrom("RabbitIn")
    .When(msg => msg.GetProperty("priority").GetString() == "high")
    .Build();
```

### Consumer Dispatch (Performance)

`ConsumerDispatcher` (separate from the broker, injected via DI) handles typed dispatch:
- **Compiled Expression lambdas** built once per message type and cached in `ConcurrentDictionary` — avoids `MethodInfo.Invoke`.
- **`IServiceScopeFactory`** creates a new async DI scope per dispatch — supports `Scoped` consumer lifetimes.
- A consumer can implement multiple `IConsume<T>` interfaces; `AddConsumer<T>()` registers all of them.

### Message Type Resolution

`MessageTypeRegistry` maps type names → CLR types using an `ImmutableDictionary`. Lookup order:

1. `[MessageName("short.name")]` attribute value
2. `Type.FullName`
3. `Type.AssemblyQualifiedName`

`SplitMessage` is pre-registered. All types used in routes or consumers are registered automatically at config time.

### Scheduled Actions

Interval-based actions fire on a timer. Cron-based actions use **Cronos** (`CronExpression.Parse`) with local timezone. Each action runs in a background `Task` managed by `ScheduledActionHandle` (implements `IAsyncDisposable`). A random startup jitter (100–2000 ms) staggers multiple scheduled actions.

### Aggregator / Splitter

`SplitterImpl.Split(byte[], ISplitCondition)` partitions large payloads into `SplitMessage` parts (Base64 chunks, shared `CorrelationId`). `AggregatorImpl` collects parts by correlation ID and returns reassembled bytes when `GroupSize` is met. Incomplete aggregates expire after 2 hours.

### Factory / DI

```csharp
// Core only (no RabbitMQ dependency):
services.AddMessageBroker()
    .AddFileEndPoint("In",  new FileSettings { ReadPath = "in",  PostPath = "in-out" })
    .AddFileEndPoint("Out", new FileSettings { ReadPath = "out", PostPath = "out-out" })
    .AddMemoryEndPoint("Mem")
    .AddConsumer<OrderConsumer>()   // implements IConsume<OrderMessage>
    .Build();

// With RabbitMQ (reference MessageBroker.RabbitMq project):
services.AddMessageBroker()
    .AddRabbitMqEndPoint("Rabbit", new RabbitMqSettings { HostName = "localhost", ReadQueueName = "q.in" })
    .AddMemoryEndPoint("Mem")
    .AddConsumer<OrderConsumer>()
    .Build();
// IMessageBroker registered as singleton; MessageBrokerHostedService registered as IHostedService.
```

Or from a config file (RabbitMq entries require calling `WithRabbitMq()`):

```csharp
services.AddMessageBroker()
    .LoadConfiguration("queuesettings.json")
    .WithRabbitMq()               // from MessageBroker.RabbitMq — processes RabbitMq endpoints
    .AddConsumer<OrderConsumer>()
    .Build();
```

Config section key is `MessageBroker` → `Endpoints[]` with `Name`, `Type` (`File|RabbitMq|Memory`), `Config` (camelCase type-specific settings).

`MessageBrokerBuilder` exposes `Services` (the DI container) and `LoadedConfiguration` as public properties so extension packages in other assemblies can register their endpoint types.

### Performance Design

- **RecyclableMemoryStream** (`Microsoft.IO.RecyclableMemoryStream`) for all serialization streams.
- **Compiled dispatch lambdas** in `ConsumerDispatcher` — `~10×` faster than `MethodInfo.Invoke`.
- **ImmutableList / ImmutableDictionary** for routes, filters, consumer keys — lock-free reads; writes (config-time only) use atomic replacement.
- **PropertyInfo cache** in `MessageSerializerJson.PropCache` — one reflection lookup per concrete `MessageContext<T>` type.
- **Bounded `Channel<string>`** in `MemoryQueueEndPoint` for backpressure.

## Key Design Rules

- **Ask before architectural decisions** — built incrementally with explicit sign-off on each structural choice.
- Routes use `IRouteCondition` / `Func<JsonElement, bool>` predicates; no XML/XSLT.
- No Windows-specific endpoints (MSMQ, Event Log, etc.) — .NET Core only.
- DI: `Microsoft.Extensions.DI` only (no Autofac).
- Serialization: `System.Text.Json` only (no Newtonsoft).
- Consumers are keyed services: key = `typeof(TConsumer).Name`.
- `RouteContext` is non-sealed and `Evaluate()` is virtual — subclass for custom route logic.
