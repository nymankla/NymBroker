# Writing an Endpoint

An **endpoint** connects NymBroker to a transport: a queue, a database table, a socket, a folder. The broker hands an endpoint bytes to send, and the endpoint hands the broker bytes it has received. The broker does everything else: serialization, routing, topics, consumers, splitting and dead-lettering.

This guide covers the contract, the three endpoint shapes, registration, configuration and testing. Each shape has a complete sample.

- [1. The contract](#1-the-contract)
- [2. Choose a shape](#2-choose-a-shape)
- [3. Sample A — write-only sink](#3-sample-a--write-only-sink)
- [4. Sample B — push-based listener](#4-sample-b--push-based-listener)
- [5. Sample C — pull-based poller with ack/abandon](#5-sample-c--pull-based-poller-with-ackabandon)
- [6. Registering with the builder](#6-registering-with-the-builder)
- [7. Loading from `queuesettings.json`](#7-loading-from-queuesettingsjson)
- [8. Testing](#8-testing)
- [9. Checklist](#9-checklist)

---

## 1. The contract

Two interfaces in `NymBroker.Core.Endpoint`:

```csharp
public interface IEndPoint
{
    EndpointMode Mode => EndpointMode.ReadWrite;          // ReadWrite | ReadOnly | WriteOnly
    Task PostAsync(byte[] message, CancellationToken ct = default);
    IHealthCheckResult HealthCheck();
}

public interface IEndPointEventDriven : IEndPoint
{
    Task StartListeningAsync(Func<byte[], CancellationToken, Task> handler, CancellationToken ct);
    Task StopListeningAsync();
}
```

### What the broker does with your endpoint

| When | The broker calls | Your job |
|---|---|---|
| A route, topic, wire tap, dead-letter or `PostAsync` targets the endpoint | `PostAsync(bytes, ct)` | Deliver the bytes durably (or throw). |
| `StartAsync` (host start), if the endpoint is `IEndPointEventDriven` and not `WriteOnly` | `StartListeningAsync(handler, ct)` | Start receiving **in the background** and return quickly. Call `handler(bytes, ct)` once per received message. |
| `StopAsync` (host stop) | `StopListeningAsync()` | Stop receiving. Let in-flight messages finish. |
| Health probes / your own code | `HealthCheck()` | Report whether the transport is usable. Never throw. |

The `handler` the broker passes in is `ProcessAsync(raw, endpointName, ct)`. The source name is already bound to it, so routes like `.WhenFrom("MyEndpoint")` work without any extra code.

### The bytes are opaque

`PostAsync` receives a fully serialized UTF-8 JSON envelope (`id`, `correlationId`, `address`, `messageType`, `created`, `message`). Store or transmit those bytes **unchanged**, and give the same bytes back to `handler` on the receiving side. Don't parse or re-encode them: the broker owns the format. That includes `SplitMessage` parts and compressed payloads.

If your transport has a size limit (UDP datagrams, some queue services), callers pass `splitThresholdBytes` to `broker.PostAsync`. The broker then splits the message into parts your endpoint can carry, and reassembles them on the receiving side. The endpoint needs no special handling.

### What happens when `handler` throws

`ProcessAsync` deals with most failures itself, so they don't reach your endpoint:

- Deserialization failures are logged and the message is dropped.
- Consumer and topic fan-out failures are logged and the message is sent to the dead-letter endpoint, if one is configured.

The handler **can** still throw. For example, a route's destination `PostAsync` may fail, or the token may be cancelled. When the handler throws, treat the message as **not processed**:

| Transport has redelivery? | On handler failure |
|---|---|
| Yes (RabbitMQ, SQL lease tables, cloud queues) | Log at `Error`, then nack, abandon or release the lease so the message is redelivered. |
| No (UDP, in-memory, webhooks without retry) | Log at `Error` and move on. The message is lost; say so in your docs. |

In both cases, **keep the listener loop alive**. One bad message must never stop the endpoint.

### `EndpointMode`

Accept a `mode` constructor parameter (default `ReadWrite`) and expose it as `Mode`. The broker enforces it:

- **`WriteOnly`**: `StartListeningAsync` is never called.
- **`ReadOnly`**: posting to the endpoint throws `InvalidOperationException`. `StartAsync` fails fast if a route, topic, dead-letter or wire-tap target is read-only.

Your endpoint doesn't need to check `Mode` itself.

---

## 2. Choose a shape

| Shape | Implement | Receive model | Existing examples |
|---|---|---|---|
| **Sink** (send only) | `IEndPoint`, `Mode => WriteOnly` | none | (Sample A) |
| **Push** | `IEndPointEventDriven` | The transport calls you (socket, SDK callback, file watcher) | `MemoryQueueEndPoint`, `FileEndPoint`, `RabbitMqEndPoint` |
| **Pull** | `IEndPointEventDriven` | You poll in a background loop and call `handler` per message | `SqliteEndPoint`, `PostgresEndPoint` |

> There is no separate polling interface. A pull-based transport still implements `IEndPointEventDriven` and runs its own poll loop inside `StartListeningAsync`, so the broker drives every receiving endpoint the same way.

---

## 3. Sample A — write-only sink

This is the smallest possible endpoint. It writes each envelope as one line, for example to an audit log or to stdout for a container log collector.

```csharp
using System.Text;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;

namespace MyCompany.NymBroker.Console;

/// <summary>Write-only sink: every posted envelope is written as one line of JSON.</summary>
public sealed class ConsoleSinkEndPoint : IEndPoint
{
    private readonly TextWriter _writer;
    private readonly SemaphoreSlim _writeLock = new(1, 1);

    public ConsoleSinkEndPoint(TextWriter? writer = null) => _writer = writer ?? System.Console.Out;

    public EndpointMode Mode => EndpointMode.WriteOnly;

    public async Task PostAsync(byte[] message, CancellationToken ct = default)
    {
        await _writeLock.WaitAsync(ct);
        try
        {
            await _writer.WriteLineAsync(Encoding.UTF8.GetString(message).AsMemory(), ct);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();
}
```

Points to note:

- **`PostAsync` can run concurrently.** Routes, topics and wire taps may post from several messages at once, and `TextWriter` isn't thread-safe, so the writes go through a lock. Use whatever your client library needs: a lock, a pooled channel, or nothing if the client is already thread-safe.
- **Hard-coded `WriteOnly`.** The endpoint has no receive side, so it doesn't take a `mode` parameter.

Use it as an audit trail with a wire tap: `.AddWireTap("Audit")`.

---

## 4. Sample B — push-based listener

UDP is a good minimal example of a push transport: it needs no dependencies, data arrives on its own, and there is no acknowledgement.

```csharp
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;

namespace MyCompany.NymBroker.Udp;

public sealed class UdpSettings
{
    /// <summary>Local port to listen on. 0 disables listening.</summary>
    public int ListenPort { get; set; }

    /// <summary>Host that <see cref="UdpEndPoint.PostAsync"/> sends to.</summary>
    public string RemoteHost { get; set; } = "127.0.0.1";

    public int RemotePort { get; set; }
}

/// <summary>Push-based endpoint: one UDP datagram = one message envelope. Fire-and-forget, no acks.</summary>
public sealed class UdpEndPoint : IEndPointEventDriven, IAsyncDisposable
{
    private readonly string _name;
    private readonly UdpSettings _settings;
    private readonly ILogger<UdpEndPoint> _logger;
    private readonly UdpClient _sender = new();
    private UdpClient? _listener;
    private CancellationTokenSource? _listeningCts;
    private Task? _loop;

    public UdpEndPoint(string name, UdpSettings settings, ILogger<UdpEndPoint> logger, EndpointMode mode = EndpointMode.ReadWrite)
    {
        _name = name;
        _settings = settings;
        _logger = logger;
        Mode = mode;
    }

    public EndpointMode Mode { get; }

    public async Task PostAsync(byte[] message, CancellationToken ct = default)
    {
        // Datagrams max out near 64 KB — callers should pass splitThresholdBytes to PostAsync for large messages.
        await _sender.SendAsync(message, _settings.RemoteHost, _settings.RemotePort, ct);
    }

    public Task StartListeningAsync(Func<byte[], CancellationToken, Task> handler, CancellationToken ct)
    {
        _listener = new UdpClient(new IPEndPoint(IPAddress.Any, _settings.ListenPort));
        _listeningCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = _listeningCts.Token;

        // Return immediately; the broker awaits StartListeningAsync during startup.
        _loop = Task.Run(() => RunListenerLoopAsync(_listener, handler, token), CancellationToken.None);
        return Task.CompletedTask;
    }

    private async Task RunListenerLoopAsync(UdpClient listener, Func<byte[], CancellationToken, Task> handler, CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                var result = await listener.ReceiveAsync(token);
                try
                {
                    await handler(result.Buffer, token);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    // UDP has no redelivery, so a failed message is lost — log it, keep the loop alive.
                    _logger.LogError(ex, "Unhandled error dispatching message on endpoint '{Name}'", _name);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Listener loop for endpoint '{Name}' terminated unexpectedly", _name);
        }
    }

    public async Task StopListeningAsync()
    {
        _listeningCts?.Cancel();
        if (_loop is not null)
            await _loop;
        _listener?.Dispose();
        _listener = null;
    }

    public IHealthCheckResult HealthCheck()
        => _listeningCts is { IsCancellationRequested: false } && _loop is { IsCompleted: true }
            ? HealthCheckResult.Unhealthy($"Listener loop for '{_name}' has stopped")
            : HealthCheckResult.Healthy();

    public async ValueTask DisposeAsync()
    {
        await StopListeningAsync();
        _listeningCts?.Dispose();
        _sender.Dispose();
    }
}
```

The patterns every listening endpoint should copy:

1. **Return from `StartListeningAsync` right away.** The broker starts endpoints one after another inside `StartAsync`. Run the receive loop with `Task.Run(..., CancellationToken.None)` and keep the `Task`.
2. **Use a linked `CancellationTokenSource`.** Then the loop stops either when the host token is cancelled or when `StopListeningAsync` runs.
3. **Use three catch layers**, the same as the built-in endpoints:
   - Per message: `catch (Exception ex) when (ex is not OperationCanceledException)` → `LogError`, then continue.
   - Loop: `catch (OperationCanceledException) { }`, because that is a clean shutdown, not an error.
   - Anything else that kills the loop → `LogCritical`.
4. **Await the loop in `StopListeningAsync`,** so shutdown doesn't cut off a message mid-dispatch.
5. **Report a dead loop in `HealthCheck`.** A listener that has stopped while it should be running is unhealthy.

---

## 5. Sample C — pull-based poller with ack/abandon

Most real brokers and cloud queues (Azure Service Bus, Amazon SQS, Google Pub/Sub, database tables) follow the same pattern: **receive with a lease**, process, then **complete** or **abandon**. This sample codes against a small `IRemoteQueueClient` interface; in a real endpoint you'd call the vendor SDK in its place.

```csharp
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Resilience;

namespace MyCompany.NymBroker.Queue;

/// <summary>Stand-in for a vendor SDK (Service Bus, SQS, ...): receive with a lease, then complete or abandon.</summary>
public interface IRemoteQueueClient
{
    Task SendAsync(byte[] body, CancellationToken ct);
    Task<IReadOnlyList<RemoteMessage>> ReceiveBatchAsync(int maxMessages, TimeSpan wait, CancellationToken ct);
    Task CompleteAsync(RemoteMessage message, CancellationToken ct);
    Task AbandonAsync(RemoteMessage message, CancellationToken ct);
    Task PingAsync(CancellationToken ct);
}

public sealed record RemoteMessage(string LockToken, byte[] Body, int DeliveryCount);

public sealed class BrokeredQueueSettings
{
    public int BatchSize { get; set; } = 32;
    public TimeSpan PollWait { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan ErrorBackoff { get; set; } = TimeSpan.FromSeconds(2);
}

/// <summary>Pull-based endpoint: a background loop polls the queue, dispatches, then acks or abandons each message.</summary>
public sealed class BrokeredQueueEndPoint : IEndPointEventDriven
{
    private readonly string _name;
    private readonly IRemoteQueueClient _client;
    private readonly BrokeredQueueSettings _settings;
    private readonly ILogger<BrokeredQueueEndPoint> _logger;
    private readonly RetryPolicy _sendPolicy;
    private CancellationTokenSource? _listeningCts;
    private Task? _loop;

    public BrokeredQueueEndPoint(string name, IRemoteQueueClient client, BrokeredQueueSettings settings,
        ILogger<BrokeredQueueEndPoint> logger, EndpointMode mode = EndpointMode.ReadWrite)
    {
        _name = name;
        _client = client;
        _settings = settings;
        _logger = logger;
        Mode = mode;

        // One policy per endpoint, reused for every send. OnRetry logs — no silent retries.
        _sendPolicy = new RetryPolicy(new RetryOptions
        {
            MaxRetryAttempts = 3,
            Delay = TimeSpan.FromMilliseconds(200),
            BackoffType = RetryBackoffType.Exponential,
            UseJitter = true,
            OnRetry = args =>
            {
                _logger.LogWarning("Send on endpoint '{Name}' failed (attempt {Attempt}), retrying in {Delay}: {Error}",
                    _name, args.AttemptNumber + 1, args.Delay, args.Exception.Message);
                return ValueTask.CompletedTask;
            }
        });
    }

    public EndpointMode Mode { get; }

    public Task PostAsync(byte[] message, CancellationToken ct = default)
        => _sendPolicy.ExecuteAsync(token => new ValueTask(_client.SendAsync(message, token)), ct).AsTask();

    public Task StartListeningAsync(Func<byte[], CancellationToken, Task> handler, CancellationToken ct)
    {
        _listeningCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = _listeningCts.Token;
        _loop = Task.Run(() => RunListenerLoopAsync(handler, token), CancellationToken.None);
        return Task.CompletedTask;
    }

    private async Task RunListenerLoopAsync(Func<byte[], CancellationToken, Task> handler, CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    var batch = await _client.ReceiveBatchAsync(_settings.BatchSize, _settings.PollWait, token);
                    foreach (var message in batch)
                        await DispatchAsync(message, handler, token);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    // Poll failure (network, auth, ...) — log, back off, keep polling.
                    _logger.LogError(ex, "Poll error on endpoint '{Name}'", _name);
                    await Task.Delay(_settings.ErrorBackoff, token);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Listener loop for endpoint '{Name}' terminated unexpectedly", _name);
        }
    }

    private async Task DispatchAsync(RemoteMessage message, Func<byte[], CancellationToken, Task> handler, CancellationToken token)
    {
        try
        {
            await handler(message.Body, token);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // The broker did not finish with the message — hand it back so the queue redelivers it.
            _logger.LogError(ex, "Unhandled error dispatching message on endpoint '{Name}' (delivery {Count})",
                _name, message.DeliveryCount);
            await _client.AbandonAsync(message, token);
            return;
        }

        await _client.CompleteAsync(message, token);
    }

    public async Task StopListeningAsync()
    {
        _listeningCts?.Cancel();
        if (_loop is not null)
            await _loop;
        _listeningCts?.Dispose();
        _listeningCts = null;
    }

    public IHealthCheckResult HealthCheck()
    {
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            _client.PingAsync(cts.Token).GetAwaiter().GetResult();
            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Health check failed for endpoint '{Name}'", _name);
            return HealthCheckResult.Unhealthy(ex.Message);
        }
    }
}
```

What this sample adds beyond Sample B:

- **Ack only after the handler succeeds.** `CompleteAsync` runs only when `handler` returned normally. If the process crashes mid-dispatch, the lease expires and the queue redelivers the message. This gives you *at-least-once* delivery. Pair it with `.AddIdempotentReceiver()` on the builder if consumers must not see duplicates.
- **Separate poll errors from message errors.** A failed `ReceiveBatchAsync` is a transport problem: log it, back off, retry. A failed `handler` is a message problem: abandon it and continue with the batch.
- **Handle poison messages.** Repeated abandons should eventually stop. Rely on the queue's max-delivery-count / dead-letter feature if it has one. Otherwise, check `DeliveryCount` and mark the message failed yourself, the way `SqliteEndPoint` and `PostgresEndPoint` use `MaxRetryCount`, and log at `Warning` when you do.
- **Retry with `RetryPolicy`.** Use `NymBroker.Resilience` for transient send and reconnect failures. Build one policy per endpoint and reuse it; `OnRetry` must log. Don't add Polly. See [NymBroker.Resilience/README.md](../NymBroker.Resilience/README.md) for every option.
- **Keep the health check bounded.** `HealthCheck()` is synchronous, so put a timeout on the probe and catch everything. A health check must never throw.

> **Concurrency:** this loop dispatches one message at a time, which keeps ordering and is the right default. If you add parallel dispatch (for example a `SemaphoreSlim(n)`), document that ordering is no longer guaranteed. If your client isn't thread-safe, serialize access to it the way `SqliteEndPoint` does with `_dbLock`.

---

## 6. Registering with the builder

Endpoints are **keyed singletons** of `IEndPoint`, keyed by endpoint name. The builder also has to know the name, so that `Build()` attaches the endpoint to the broker. Ship a builder extension so callers get the same one-liner as the built-in transports:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Factory;

namespace MyCompany.NymBroker.Udp;

public static class NymBrokerBuilderUdpExtensions
{
    public static NymBrokerBuilder AddUdpEndPoint(
        this NymBrokerBuilder builder, string name, UdpSettings settings,
        EndpointMode mode = EndpointMode.ReadWrite)
    {
        builder.Services.AddKeyedSingleton<IEndPoint>(name,
            (sp, _) => new UdpEndPoint(name, settings, sp.GetRequiredService<ILogger<UdpEndPoint>>(), mode));
        builder.RegisterEndpoint(name);
        return builder;
    }
}
```

Both calls are required:

| Call | Without it |
|---|---|
| `Services.AddKeyedSingleton<IEndPoint>(name, …)` | `Build()` fails to resolve the endpoint at startup. |
| `builder.RegisterEndpoint(name)` | The endpoint exists in DI but the broker never sees it: no listener, and posts to it fail with *"No endpoint registered with name …"*. |

Resolve your dependencies (loggers, SDK clients, `IOptions<T>`) inside the factory lambda with `sp`, so they come from the app's container.

Usage:

```csharp
builder.Services.AddNymBroker()
    .AddUdpEndPoint("UdpIn", new UdpSettings { ListenPort = 5000, RemoteHost = "10.0.0.5", RemotePort = 5000 })
    .AddMemoryEndPoint("Work")
    .AddConsumer<PingConsumer>()
    .Build();

// Anywhere with INymBroker injected:
await broker.PostAsync("UdpIn", new Ping { Text = "hello" });
```

The hosted service starts the broker, and the broker starts your listener. You don't need to call `StartListeningAsync` yourself.

### Where to put it

- **In this repo:** create a `NymBroker.<Transport>` project that references only `NymBroker.Core` plus the transport's client library, the same layout as `NymBroker.RabbitMq`, `NymBroker.Sqlite` and `NymBroker.Postgres`. Add it to `scripts/pack.ps1` so it ships with the other packages.
- **In your own app or package:** reference the `NymBroker` package and copy the same pattern. Everything you need (`NymBrokerBuilder.Services`, `RegisterEndpoint`, `LoadedConfiguration`) is public.

---

## 7. Loading from `queuesettings.json`

Config-driven endpoints follow a `With<Transport>()` pattern. It reads the matching entries from `builder.LoadedConfiguration` and calls your `Add…EndPoint`:

```csharp
public static NymBrokerBuilder WithUdp(this NymBrokerBuilder builder)
{
    if (builder.LoadedConfiguration is null) return builder;

    foreach (var ep in builder.LoadedConfiguration.Endpoints)
    {
        if (ep.Type == EndPointType.Udp)   // needs a new EndPointType member — see the note below
            builder.AddUdpEndPoint(ep.Name, ToSettings(ep), ep.Mode);
    }

    return builder;
}

private static readonly JsonSerializerOptions JsonOptions = new()
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    PropertyNameCaseInsensitive = true
};

private static UdpSettings ToSettings(EndPointConfiguration ep)
    => ep.Config.HasValue
        ? JsonSerializer.Deserialize<UdpSettings>(ep.Config.Value.GetRawText(), JsonOptions) ?? new()
        : new();
```

```json
{
  "NymBroker": {
    "Endpoints": [
      { "Name": "UdpIn", "Type": "Udp", "Config": { "listenPort": 5000, "remoteHost": "10.0.0.5", "remotePort": 5000 } }
    ]
  }
}
```

> **`EndPointType` is a closed enum.** `Type` is parsed into `NymBroker.Core.Factory.EndPointType`, which currently has `File`, `RabbitMq`, `Memory`, `Sql` and `Postgres`. An unknown string makes `LoadConfiguration` throw, and that breaks every endpoint in the file, not just yours. So config support for a new transport needs a new enum member in `NymBroker.Core`, which is a core change that needs sign-off. Until then, register the endpoint in code with `Add…EndPoint`.

---

## 8. Testing

Endpoint tests live in `NymBroker.Tests` and must not need external infrastructure: no RabbitMQ, no Postgres, no real file I/O. Inject a fake for the client (as `IRemoteQueueClient` allows in Sample C), or use a local-only resource such as a loopback socket or SQLite `:memory:`.

**Round-trip through the endpoint:**

```csharp
[Fact]
public async Task PostedMessage_IsDeliveredToHandler()
{
    await using var ep = new UdpEndPoint("Udp",
        new UdpSettings { ListenPort = 50511, RemotePort = 50511 }, NullLogger<UdpEndPoint>.Instance);

    var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
    await ep.StartListeningAsync((raw, _) => { received.TrySetResult(raw); return Task.CompletedTask; },
        TestContext.Current.CancellationToken);

    var payload = Encoding.UTF8.GetBytes("""{"id":"1"}""");
    await ep.PostAsync(payload, TestContext.Current.CancellationToken);

    Assert.Equal(payload, await received.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
}
```

**Failure semantics.** With a fake client, assert that:

- a handler that throws leads to `AbandonAsync`, not `CompleteAsync`;
- the listener keeps going and processes the next message;
- a poll exception is logged and polling resumes;
- `StopListeningAsync` returns and no handler call happens after it.

**Through the broker.** Register the endpoint with `NymBrokerImpl.AddEndpoint(name, endpoint)`, add a route or consumer, call `StartAsync`, and post. See `MessageFlowTests` and `ObservabilityTests` for how to build an `NymBrokerImpl` by hand.

Tests run in parallel. Use a unique name, port or table per test, and filter anything process-wide (activity listeners, meters) by your endpoint's name.

---

## 9. Checklist

- [ ] Implements `IEndPointEventDriven` if it receives anything (pull transports run their poll loop inside it); a pure sink implements `IEndPoint` with `Mode => WriteOnly`.
- [ ] Constructor takes `string name`, a settings object, `ILogger<T>` and `EndpointMode mode = EndpointMode.ReadWrite`.
- [ ] `PostAsync` sends the bytes unchanged, is safe to call concurrently, and honours `ct`.
- [ ] `StartListeningAsync` returns immediately; the loop runs on `Task.Run` with a linked CTS.
- [ ] Every received message goes through `handler`. On handler failure: `LogError`, then nack/abandon (or drop, if the transport can't redeliver). The loop survives.
- [ ] Messages are acked/completed only **after** the handler succeeds.
- [ ] Poison messages stop being retried eventually (max delivery count or `MaxRetryCount`), with a `Warning` log.
- [ ] `OperationCanceledException` is swallowed only at shutdown boundaries; unexpected loop death is logged at `Critical`.
- [ ] No silent catches: every `catch` logs.
- [ ] `StopListeningAsync` stops receiving and awaits the loop.
- [ ] `HealthCheck()` is bounded, never throws, and reports a dead listener.
- [ ] Transient failures use `NymBroker.Resilience.RetryPolicy` with a logging `OnRetry`.
- [ ] Builder extension calls both `Services.AddKeyedSingleton<IEndPoint>(name, …)` and `RegisterEndpoint(name)`.
- [ ] Tests use no external infrastructure and are safe to run in parallel.
- [ ] No Windows-only APIs, `System.Text.Json` only, `Microsoft.Extensions.DependencyInjection` only.
