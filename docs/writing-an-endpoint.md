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

    // Optional: send several envelopes, in order. The default calls PostAsync for each.
    Task PostBatchAsync(IReadOnlyList<byte[]> messages, CancellationToken ct = default);
}

public interface IEndPointEventDriven : IEndPoint
{
    Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct);
    Task StopListeningAsync();
    bool UsesNativeDeadLetter => false;   // true if the transport can dead-letter a received message itself
}

public enum ProcessOutcome { Completed, Retry, DeadLetter }

public readonly record struct ProcessResult(ProcessOutcome Outcome, string? Reason, string? Description, Exception? Exception)
{
    public string? FailureText { get; }   // "{Reason}: {Description}" — for logs and error columns
}
```

### What the broker does with your endpoint

| When | The broker calls | Your job |
|---|---|---|
| A route, topic, wire tap, dead-letter or `PostAsync` targets the endpoint | `PostAsync(bytes, ct)` | Deliver the bytes durably (or throw). |
| `StartAsync` (host start), if the endpoint is `IEndPointEventDriven` and not `WriteOnly` | `StartListeningAsync(handler, ct)` | Start receiving **in the background** and return quickly. Call `handler(bytes, ct)` once per received message and **settle the message by the `ProcessResult` it returns**. |
| `StopAsync` (host stop) | `StopListeningAsync()` | Stop receiving. Let in-flight messages finish. |
| Health probes / your own code | `HealthCheck()` | Report whether the transport is usable. Never throw. |
| `broker.PostBatchAsync` / topic fan-out of `PublishBatchAsync` | `PostBatchAsync(envelopes, ct)` | Optional override: send all envelopes in order, in as few round trips as the transport allows. Each is still received on its own. |

The `handler` the broker passes in is `ProcessAsync(raw, endpointName, ct)`. The source name is already bound to it, so routes like `.WhenFrom("MyEndpoint")` work without any extra code.

### The bytes are opaque

`PostAsync` receives a fully serialized UTF-8 JSON envelope (`id`, `correlationId`, `address`, `messageType`, `created`, `message`). Store or transmit those bytes **unchanged**, and give the same bytes back to `handler` on the receiving side. Don't parse or re-encode them: the broker owns the format. That includes `SplitMessage` parts and compressed payloads.

If your transport has a size limit (UDP datagrams, some queue services), callers pass `splitThresholdBytes` to `broker.PostAsync`. The broker then splits the message into parts your endpoint can carry, and reassembles them on the receiving side. The endpoint needs no special handling.

### Settling a message: the `ProcessResult`

The handler returns a `ProcessResult` that says what to do with the message you received. Map each outcome to your transport's own operation:

| Outcome | Meaning | Transport with redelivery (RabbitMQ, SQL lease tables, cloud queues) | No redelivery (UDP, in-memory, webhooks) |
|---|---|---|---|
| `Completed` | Done | ack / complete / mark completed | nothing to do |
| `Retry` | Failed, may succeed later (a consumer threw, a route's destination failed) | nack / abandon / release the lease so it is **redelivered**; stop after a max delivery count | log at `Error`: the message is lost |
| `DeadLetter` | Can never succeed (`Reason` is one of `DeadLetterReasons`: undecodable, expired, …) | move it to the transport's **dead-letter queue** now, recording `Reason` / `Description` | never returned (see below) |

- **`ProcessAsync` doesn't throw** (except on cancellation), but still wrap the handler call in a try/catch and treat an exception as `Retry`.
- **Keep the listener loop alive** whatever the outcome. One bad message must never stop the endpoint.
- **`ProcessResult.FailureText`** (`"{Reason}: {Description}"`) is the string to put in logs and error columns.

### Native dead-lettering: `UsesNativeDeadLetter`

If your transport has its own dead-letter queue (a dead-letter exchange, a DLQ sub-queue, a `Failed` state in a table), return `true` from `UsesNativeDeadLetter`, driven by a `UseNativeDeadLetter` setting that defaults to `true`. That changes what the broker returns for failures:

| Failure | `UsesNativeDeadLetter = true` | `false` (the default for the interface) |
|---|---|---|
| Consumer or topic delivery throws | `Retry`: your transport redelivers and dead-letters at its delivery limit | the broker posts the bytes to its dead-letter endpoint (`WithDeadLetterEndpoint`) and returns `Completed` |
| Undecodable, expired, unknown compression | `DeadLetter(reason)`: dead-letter it now | the broker posts to its dead-letter endpoint and returns `Completed` |
| A route's destination throws | `Retry` | `Retry` |

When your endpoint *reads* a native dead-letter queue, call `DeadLetterEnvelope.Annotate(body, new DeadLetterInfo(reason, description, null, sourceEndpoint, deadLetteredAt, deliveryCount))` on each received body before handing it to the broker. The block (`context.DeadLetter`) is then the same whether the message came from the broker's dead-letter endpoint or the transport's own queue.

So an endpoint **without** a native dead-letter queue only ever sees `Completed` or `Retry`, and failed messages end up on the broker's dead-letter endpoint. Endpoints in this repo: RabbitMQ (reject without requeue, i.e. the queue's dead-letter exchange), Azure Service Bus (`DeadLetterMessageAsync` with the reason) and the SQLite / PostgreSQL / SQL Server tables (`Failed` with the reason in the error column) dead-letter natively; Memory and File don't.

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

    public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct)
    {
        _listener = new UdpClient(new IPEndPoint(IPAddress.Any, _settings.ListenPort));
        _listeningCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = _listeningCts.Token;

        // Return immediately; the broker awaits StartListeningAsync during startup.
        _loop = Task.Run(() => RunListenerLoopAsync(_listener, handler, token), CancellationToken.None);
        return Task.CompletedTask;
    }

    private async Task RunListenerLoopAsync(UdpClient listener, Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested)
            {
                var datagram = await listener.ReceiveAsync(token);

                // A handler exception is logged and comes back as Retry; only the stop token's cancellation is rethrown.
                var result = await EndpointHandler.InvokeAsync(handler, datagram.Buffer, _logger, _name, token);

                // UDP has no redelivery and no dead-letter queue (UsesNativeDeadLetter stays false), so anything
                // but Completed means the message is lost — log it and keep the loop alive.
                if (result.Outcome != ProcessOutcome.Completed)
                    _logger.LogError(result.Exception, "Message on endpoint '{Name}' was not processed ({Failure}) and cannot be redelivered",
                        _name, result.FailureText);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
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
3. **Settle every message by its result.** UDP can't redeliver or dead-letter, so `UsesNativeDeadLetter` stays `false` and anything other than `Completed` is logged as lost.
4. **Use three catch layers**, the same as the built-in endpoints:
   - Per message: call the handler through `EndpointHandler.InvokeAsync` (`NymBroker.Core.Endpoint`). It logs a handler exception at `Error` and returns `Retry`, and rethrows only an `OperationCanceledException` while your stop token is cancelled. A cancellation the handler raises itself, such as an HTTP timeout, is a failure, not a shutdown.
   - Loop: `catch (OperationCanceledException) when (token.IsCancellationRequested) { }`, because that is a clean shutdown, not an error.
   - Anything else that kills the loop → `LogCritical`.
5. **Await the loop in `StopListeningAsync`,** so shutdown doesn't cut off a message mid-dispatch.
6. **Report a dead loop in `HealthCheck`.** A listener that has stopped while it should be running is unhealthy.

---

## 5. Sample C — pull-based poller with ack/abandon

Most real brokers and cloud queues (Azure Service Bus, Amazon SQS, Google Pub/Sub, database tables) follow the same pattern: **receive with a lease**, process, then **complete**, **abandon** or **dead-letter**. This sample codes against a small `IRemoteQueueClient` interface; in a real endpoint you'd call the vendor SDK in its place.

```csharp
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Resilience;

namespace MyCompany.NymBroker.Queue;

/// <summary>Stand-in for a vendor SDK (Service Bus, SQS, ...): receive with a lease, then complete or abandon.</summary>
public interface IRemoteQueueClient
{
    Task SendAsync(byte[] body, CancellationToken ct);
    Task<IReadOnlyList<RemoteMessage>> ReceiveBatchAsync(int maxMessages, TimeSpan wait, CancellationToken ct);
    Task CompleteAsync(RemoteMessage message, CancellationToken ct);
    Task AbandonAsync(RemoteMessage message, CancellationToken ct);
    Task DeadLetterAsync(RemoteMessage message, string reason, string? description, CancellationToken ct);
    Task PingAsync(CancellationToken ct);
}

public sealed record RemoteMessage(string LockToken, byte[] Body, int DeliveryCount);

public sealed class BrokeredQueueSettings
{
    public int BatchSize { get; set; } = 32;
    public TimeSpan PollWait { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan ErrorBackoff { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Let the queue's own dead-letter queue hold failed messages (instead of the broker's dead-letter endpoint).</summary>
    public bool UseNativeDeadLetter { get; set; } = true;
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

    /// <summary>The queue has its own dead-letter queue, so the broker hands failures back as Retry / DeadLetter.</summary>
    public bool UsesNativeDeadLetter => _settings.UseNativeDeadLetter;

    public Task PostAsync(byte[] message, CancellationToken ct = default)
        => _sendPolicy.ExecuteAsync(token => new ValueTask(_client.SendAsync(message, token)), ct).AsTask();

    public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct)
    {
        _listeningCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = _listeningCts.Token;
        _loop = Task.Run(() => RunListenerLoopAsync(handler, token), CancellationToken.None);
        return Task.CompletedTask;
    }

    private async Task RunListenerLoopAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken token)
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

    private async Task DispatchAsync(RemoteMessage message, Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken token)
    {
        // Defensive: ProcessAsync does not throw, but a handler might — InvokeAsync logs it and returns Retry.
        var result = await EndpointHandler.InvokeAsync(handler, message.Body, _logger, _name, token, message.LockToken);

        switch (result.Outcome)
        {
            case ProcessOutcome.Completed:
                await _client.CompleteAsync(message, token);
                break;

            case ProcessOutcome.DeadLetter:
                // The message can never succeed — move it to the queue's dead-letter queue with the reason.
                _logger.LogWarning("Message on endpoint '{Name}' dead-lettered: {Failure}", _name, result.FailureText);
                await _client.DeadLetterAsync(message, result.Reason!, result.Description, token);
                break;

            default:
                // Retry — hand it back; the queue redelivers it and dead-letters it at its max delivery count.
                _logger.LogWarning("Message on endpoint '{Name}' will be redelivered (delivery {Count}): {Failure}",
                    _name, message.DeliveryCount, result.FailureText);
                await _client.AbandonAsync(message, token);
                break;
        }
    }

    public async Task StopListeningAsync()
    {
        _listeningCts?.Cancel();
        if (_loop is not null)
            await _loop;
        _listeningCts?.Dispose();
        _listeningCts = null;
    }

    // Runs the probe with a 5 s timeout; a failure is logged at Error and reported unhealthy. Never throws.
    public IHealthCheckResult HealthCheck()
        => HealthCheckResult.FromProbe("Brokered queue", _name, _logger, ct => _client.PingAsync(ct));
}
```

What this sample adds beyond Sample B:

- **Settle by the result, and ack only on `Completed`.** `CompleteAsync` runs only for `Completed`; `Retry` abandons the message so the queue redelivers it; `DeadLetter` moves it to the queue's dead-letter queue with the reason. The endpoint sets `UsesNativeDeadLetter` because the queue has a DLQ. If the process crashes mid-dispatch, the lease expires and the queue redelivers the message. This gives you *at-least-once* delivery. Pair it with `.AddIdempotentReceiver()` on the builder if consumers must not see duplicates.
- **Separate poll errors from message errors.** A failed `ReceiveBatchAsync` is a transport problem: log it, back off, retry. A `Retry` or `DeadLetter` result is a message problem: settle that message and continue with the batch.
- **Handle poison messages.** Repeated `Retry`s should eventually stop. Rely on the queue's max-delivery-count / dead-letter feature if it has one. Otherwise, check `DeliveryCount` and mark the message failed yourself, the way the SQL endpoints use `MaxRetryCount`, and log at `Warning` when you do.
- **Retry with `RetryPolicy`.** Use `RetryPolicy` (`NymBroker.Core.Resilience`) for transient send and reconnect failures. Build one policy per endpoint and reuse it; `OnRetry` must log. Don't add Polly. See [Retry policy](resilience.md) for every option. If the client SDK already retries transient faults (the Azure SDKs do, as in `AzureServiceBusEndPoint`), don't wrap it in a second retry loop.
- **Keep the health check bounded.** `HealthCheck()` is synchronous, so put a timeout on the probe and catch everything. A health check must never throw.
- **Drain back to back.** Wait `PollInterval` only after a poll that returned nothing. Waiting after every batch caps throughput at `BatchSize / PollInterval`: about 100 msg/s with the defaults.
- **For a database-backed queue, batch the round trips.** The SQL endpoints in this repo (`SqliteEndPoint`, `PostgresEndPoint`, `SqlServerEndPoint`) all use the same patterns:
  - claim a batch in one statement;
  - write the outcomes of batch N in the **same transaction** as the claim of batch N+1, which costs one commit per batch instead of one per message;
  - guard each outcome update with the attempt number it was claimed with, so a poller whose lease expired can't overwrite a row that was claimed again;
  - prepare statements once;
  - use literal (not parameterized) status values when the table has a filtered or partial index.

  When the loop stops, write the outcomes of messages already handled with a fresh token. Measured gains were 10× to 1 000× over a per-message design; see CLAUDE.md, *SQLite / PostgreSQL / SQL Server Endpoint*.

  **Don't write that loop yourself:** derive from `LeasedQueueListener` (`NymBroker.Core.Endpoint.Queue`), which the three SQL endpoints use. It runs the drain/idle loop, calls the handler, maps each `ProcessResult` to a row outcome (`Completed`; `DeadLetter` → `Failed`; `Retry` → `Pending`, or `Failed` at `MaxRetryCount`) with the standard log messages, and writes the handled outcomes on shutdown. You supply the data access; your settings class implements `ILeasedQueueSettings`:

  ```csharp
  private sealed class Listener(MyQueueEndPoint owner) : LeasedQueueListener(owner._name, owner._settings, owner._logger)
  {
      // Write the outcomes (may be empty) and claim the next batch, ideally in one transaction.
      protected override Task<IReadOnlyList<QueueMessage>> FinalizeAndClaimAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
          => owner.FinalizeAndClaimAsync(outcomes, ct);

      // Write the outcomes without claiming (called once when the loop stops).
      protected override Task FinalizeAsync(IReadOnlyList<QueueMessageOutcome> outcomes, CancellationToken ct)
          => owner.FinalizeAsync(outcomes, ct);

      // Optional: wake up early on a notification instead of waiting the whole PollInterval (see PostgresEndPoint).
      // protected override Task WaitWhenIdleAsync(CancellationToken ct) => ...;
  }

  // In the endpoint: StartListeningAsync → _listener.Start(handler, ct); StopListeningAsync → _listener.StopAsync().
  ```

  Status values are `QueueMessageStatus` (`Pending = 0`, `InProgress = 1`, `Completed = 2`, `Failed = 3`); `LeasedQueueListener.GetLeaseTimeoutSeconds(settings)` gives the lease for the claim statement.

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

- **In this repo:** create a `NymBroker.Endpoint.<Transport>` project (namespace `NymBroker.Endpoint.<Transport>`) that references only `NymBroker.Core` plus the transport's client library, the same layout as `NymBroker.Endpoint.RabbitMq`, `NymBroker.Endpoint.Sqlite` and `NymBroker.Endpoint.Postgres`. The folder, project file, `PackageId` and namespace all use that name. Add the project to `NymBroker.slnx` and reference it from `NymBroker.Tests`. `scripts/pack.ps1` packs every `NymBroker.*` project automatically, so it ships with the other packages without further changes.
- **In your own app or package:** reference the `NymBroker` package and copy the same pattern. Everything you need (`NymBrokerBuilder.Services`, `RegisterEndpoint`, `LoadedConfiguration`) is public.

---

## 7. Loading from `queuesettings.json`

Config-driven endpoints follow a `With<Transport>()` pattern. It reads the matching entries from `builder.LoadedConfiguration` and calls your `Add…EndPoint`; `AddConfiguredEndPoints` does the loop and `GetSettings<T>()` deserializes the entry's `Config` (camelCase, case-insensitive; a missing `Config` gives the defaults).

`Type` is an open string, so your package defines its own type name; Core needs no changes:

```csharp
public static class UdpEndPointType
{
    public const string Udp = "Udp";
}

public static NymBrokerBuilder WithUdp(this NymBrokerBuilder builder)
    => builder.AddConfiguredEndPoints(UdpEndPointType.Udp,   // case-insensitive
        ep => builder.AddUdpEndPoint(ep.Name, ep.GetSettings<UdpSettings>(), ep.Mode));
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

How the pieces fit:

- `LoadConfiguration` (or `ApplyConfiguration` with a configuration read from `IConfiguration`) loads **every** entry, whatever its `Type`, and registers only `File` and `Memory` itself. Every other entry waits for the `With*()` call that recognises its type.
- `NymBroker.Core.Factory.EndPointType` holds the built-in names as string constants (`File`, `RabbitMq`, `Memory`, `Sql`, `Postgres`) and lists them in `EndPointType.BuiltIn`. Don't add your type there; keep the constant in your own package.
- Pick a type name that won't clash with the built-ins or other packages. Matching is case-insensitive.
- An entry whose `With*()` is never called is silently ignored, so posting to that endpoint name fails at runtime with *"No endpoint registered with name …"*. Remind users to call your `With*()` after `LoadConfiguration`.

---

## 8. Testing

Endpoint tests live in `NymBroker.Tests`. The default test run must not need external infrastructure, so inject a fake for the client (as `IRemoteQueueClient` allows in Sample C), or use a local-only resource such as a loopback socket, SQLite `:memory:` or a temp file.

Tests against a real server are welcome, but **env-gated**, like `PostgresEndPointTests`, `SqlServerEndPointTests` and `AzureServiceBusEndPointTests`:
- Read a connection string from a `NYMBROKER_<TRANSPORT>_CS` environment variable.
- Start each test with `Assert.SkipUnless(!string.IsNullOrWhiteSpace(cs), "Set NYMBROKER_<TRANSPORT>_CS …")`, so tests are skipped (not failed) when the variable is not set.
- Use a unique table, queue or topic per test, so tests can run in parallel, and clean it up in `DisposeAsync`.
- Add a `scripts/setup-<transport>.ps1` (a service in `scripts/docker-compose.yml`) so the server can be started locally.

Tests that need no server, such as settings validation, a health check against an unreachable address, or SQL text, always run.

**Round-trip through the endpoint:**

```csharp
[Fact]
public async Task PostedMessage_IsDeliveredToHandler()
{
    await using var ep = new UdpEndPoint("Udp",
        new UdpSettings { ListenPort = 50511, RemotePort = 50511 }, NullLogger<UdpEndPoint>.Instance);

    var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
    await ep.StartListeningAsync((raw, _) => { received.TrySetResult(raw); return Task.FromResult(ProcessResult.Completed); },
        TestContext.Current.CancellationToken);

    var payload = Encoding.UTF8.GetBytes("""{"id":"1"}""");
    await ep.PostAsync(payload, TestContext.Current.CancellationToken);

    Assert.Equal(payload, await received.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
}
```

**Failure semantics.** With a fake client, assert that:

- each `ProcessResult` maps to the right settle call: `Completed` → `CompleteAsync`, `Retry` → `AbandonAsync`, `DeadLetter` → `DeadLetterAsync` with the reason;
- a handler that throws is treated as `Retry`;
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
- [ ] If the transport can send many messages per round trip or transaction, override `PostBatchAsync`: preserve order, keep each envelope separate, and document whether a batch is atomic.
- [ ] `StartListeningAsync` returns immediately; the loop runs on `Task.Run` with a linked CTS.
- [ ] Every received message goes through `handler` and is settled by its `ProcessResult`: `Completed` → ack; `Retry` → redeliver (or log as lost if the transport can't); `DeadLetter` → the transport's dead-letter queue with `Reason`/`Description`. A handler exception counts as `Retry`. The loop survives.
- [ ] An endpoint that reads a native dead-letter queue (like Service Bus's `ReadDeadLetterQueue`) passes each body through `DeadLetterEnvelope.Annotate(body, new DeadLetterInfo(...))` before calling `handler`, so consumers see a uniform `context.DeadLetter`.
- [ ] `UsesNativeDeadLetter` is `true` (via a `UseNativeDeadLetter` setting, default `true`) only if the transport has its own dead-letter queue.
- [ ] Messages are acked/completed only for `Completed`.
- [ ] Poison messages stop being retried eventually (max delivery count or `MaxRetryCount`), with a `Warning` log.
- [ ] `OperationCanceledException` is swallowed only at shutdown boundaries; unexpected loop death is logged at `Critical`.
- [ ] No silent catches: every `catch` logs.
- [ ] `StopListeningAsync` stops receiving and awaits the loop.
- [ ] `HealthCheck()` is bounded, never throws, and reports a dead listener.
- [ ] Transient failures use `NymBroker.Core.Resilience.RetryPolicy` with a logging `OnRetry`.
- [ ] Builder extension calls both `Services.AddKeyedSingleton<IEndPoint>(name, …)` and `RegisterEndpoint(name)`.
- [ ] Tests use no external infrastructure and are safe to run in parallel.
- [ ] No Windows-only APIs, `System.Text.Json` only, `Microsoft.Extensions.DependencyInjection` only.
