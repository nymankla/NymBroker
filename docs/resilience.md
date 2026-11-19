# Resilience: retry, timeout, circuit breaker and rate limits

[← User guide](user-guide.md)

Namespace `NymBroker.Core.Resilience` (in the `NymBroker` package) has three building blocks, with no package dependencies:

- [`RetryPolicy`](#retry-policy): retries transient failures. NymBroker's own transports use it.
- [Consumer timeout and circuit breaker](#consumer-timeout-and-circuit-breaker): `ConsumerOptions.Timeout` and `ConsumerOptions.CircuitBreaker` protect a consumer and its downstream. Both are off by default.
- [`CircuitBreaker`](#circuitbreaker): the standalone breaker the consumer option uses, for your own calls.
- [Rate and concurrency limits](#rate-and-concurrency-limits): `ConsumerOptions.RateLimit` / `ConcurrencyLimit` and `LimitSendsTo` for a destination endpoint. Over the limit, messages wait instead of failing.

There is no fallback or hedging. Endpoint sends (routes, topics, destination endpoints) can have a rate and concurrency limit, but no timeout or breaker yet.

## Retry policy

`RetryPolicy` (namespace `NymBroker.Core.Resilience`, in the `NymBroker` package) is a small retry policy with no package dependencies. It replaced Polly in NymBroker; [How NymBroker uses it](#how-nymbroker-uses-it) lists where. It retries transient failures of the transport itself; what happens when a *consumer* fails is in [Reliability](reliability.md).

> Up to 0.3.5 the retry policy was a separate `NymBroker.Resilience` project and package (namespace `NymBroker.Resilience`). It now lives in `NymBroker.Core`: drop any `NymBroker.Resilience` package reference and change `using NymBroker.Resilience;` to `using NymBroker.Core.Resilience;`. The meter name is still `NymBroker.Resilience`, so existing metrics setups keep working.

### Quick start

```csharp
using NymBroker.Core.Resilience;

// Build once (e.g. in a constructor) and reuse; RetryPolicy is immutable and thread-safe.
var policy = new RetryPolicy(new RetryOptions
{
    MaxRetryAttempts = 4,
    Delay            = TimeSpan.FromMilliseconds(25),
    BackoffType      = RetryBackoffType.Exponential,
    UseJitter        = true,
    ShouldHandle     = static ex => ex is IOException,
    OnRetry          = a =>
    {
        logger.LogDebug("Retry {Attempt} in {Delay}: {Error}", a.AttemptNumber + 1, a.Delay, a.Exception.Message);
        return ValueTask.CompletedTask;
    }
});

// Without a result
await policy.ExecuteAsync(async token => await DoWorkAsync(token), ct);

// With a result
string content = await policy.ExecuteAsync(async token => await File.ReadAllTextAsync(path, token), ct);
```

The callback receives the `CancellationToken` you pass to `ExecuteAsync`. Pass it on to the work you do inside the callback.

### `RetryOptions`

| Option | Type | Default | Description |
|---|---|---|---|
| `MaxRetryAttempts` | `int` | `3` | Number of retries **after** the first attempt, so the callback runs at most `1 + MaxRetryAttempts` times. Must be ≥ 1. Use `int.MaxValue` to retry forever (until cancelled). |
| `Delay` | `TimeSpan` | `2 s` | Base delay between attempts. `TimeSpan.Zero` retries immediately. Must not be negative. |
| `MaxDelay` | `TimeSpan?` | `null` (cap of 24 h) | Upper limit for any single delay, after backoff and jitter are applied. |
| `BackoffType` | `RetryBackoffType` | `Constant` | How the delay grows between attempts. See [Backoff and jitter](#backoff-and-jitter). |
| `UseJitter` | `bool` | `false` | Randomizes delays so callers that fail together don't all retry at the same moment. |
| `ShouldHandle` | `Func<Exception, bool>` | any exception except `OperationCanceledException` | Decides whether an exception is retried. Return `false` to let it propagate at once. |
| `OnRetry` | `Func<RetryAttempt, ValueTask>?` | `null` | Called before each retry delay. Use it for logging or metrics. |
| `TimeProvider` | `TimeProvider` | `TimeProvider.System` | Clock used for the delays. Replace it in tests to avoid waiting for real time. |

Invalid options (`MaxRetryAttempts < 1`, a negative `Delay`, a null `ShouldHandle` or `TimeProvider`) throw from the `RetryPolicy` constructor.

#### `RetryAttempt` (passed to `OnRetry`)

| Property | Description |
|---|---|
| `AttemptNumber` | 0-based number of the attempt that just failed. The first retry has `AttemptNumber = 0`, so log `AttemptNumber + 1` to show a 1-based count. |
| `Exception` | The exception that caused this retry. |
| `Delay` | How long the policy will wait before the next attempt. |

### Backoff and jitter

`attempt` below is the 0-based `AttemptNumber`.

| `BackoffType` | `UseJitter = false` | `UseJitter = true` |
|---|---|---|
| `Constant` | `Delay` | `Delay` ± 25 % (uniformly random) |
| `Exponential` | `Delay × 2^attempt` (25, 50, 100, 200 ms … for a 25 ms base) | Decorrelated jitter: grows exponentially like the column to the left, but each delay is spread randomly |

Every delay is clamped to `MaxDelay` (24 h when unset). The exponential calculation can't overflow even with `MaxRetryAttempts = int.MaxValue`; very large attempt numbers simply resolve to `MaxDelay`.

The jitter formulas follow the ones Polly v8 uses (± 25 % for constant, "decorrelated jitter backoff V2" for exponential), so timing is close to what the endpoints had before.

### Execution rules

- The first attempt runs immediately. After a failure, the policy retries only if all of these hold:
  1. retries remain (`attempt < MaxRetryAttempts`),
  2. the `CancellationToken` has not been cancelled,
  3. `ShouldHandle(exception)` returns `true`.
- If any condition fails, the exception propagates as is: same instance, original stack trace, no wrapping in `AggregateException`.
- On each retry the order is: compute delay → `OnRetry` → wait → next attempt.
- Cancelling during the wait throws `OperationCanceledException` and nothing further runs.
- An exception thrown by `OnRetry` propagates and stops the retries.

### How NymBroker uses it

| Endpoint | Settings | Behaviour |
|---|---|---|
| `FileEndPoint` | `MaxRetryAttempts = 4`, `Delay = 25 ms`, `Exponential`, `UseJitter = true`, `ShouldHandle = ex is IOException`; logs at `Debug` | Retries a file that is still locked or empty when the watcher event fires. After the last attempt the endpoint logs a warning and skips the file, which keeps its original name and is not renamed to `.processed`. |
| `RabbitMqEndPoint` | `MaxRetryAttempts = int.MaxValue`, `Delay = RabbitMqSettings.ReconnectDelaySeconds`, `Constant`, default `ShouldHandle`; logs at `Warning` | Keeps reconnecting and re-subscribing until the listener is cancelled. A channel closed or a consumer cancelled by the server (consumer timeout, protocol error, queue deleted) is handled the same way; the listener first waits for a handler still running, and unacknowledged messages are redelivered, so that message is handled twice. |
| `HttpEndPoint` | `HttpSettings.MaxRetryAttempts` (default 3) and `RetryDelay` (default 1 s), `Exponential`, `UseJitter = true`, `MaxDelay = 30 s`; only timeouts, network errors, 408, 429 and 5xx | Retries one request; then `PostAsync` throws ([HTTP notes](endpoints-and-configuration.md#http-notes)). |
| `AwsSqsEndPoint` | `MaxRetryAttempts = int.MaxValue`, `Delay = 1 s`, `Exponential`, `UseJitter = true`, `MaxDelay = 30 s`; logs at `Error` | Keeps receiving after a failed receive call until the listener is cancelled. |
| Deferred posts held by the broker | `MaxRetryAttempts = 10`, `Delay = 1 s`, `Exponential`, `MaxDelay = 1 min` (about 5 minutes in all); logs at `Warning` | Retries the send of a due message on an endpoint that cannot defer itself ([Deferring a message](sending-messages.md#deferring-a-message)). |

Cancellation is always a clean shutdown: never retried, never logged as an error.

### Testing

Use `Delay = TimeSpan.Zero` so tests don't wait, or pass a fake `TimeProvider` (for example `FakeTimeProvider` from `Microsoft.Extensions.TimeProvider.Testing`) to step through delays yourself. See `NymBroker.Tests/RetryPolicyTests.cs` for examples.

## Consumer timeout and circuit breaker

Both are options of `AddConsumer`, and they apply to every message type the consumer handles:

```csharp
using NymBroker.Core.Resilience;

services.AddNymBroker()
    .AddSqlServerEndPoint("Orders", ordersSettings)
    .AddConsumer<InvoiceConsumer>(o =>
    {
        o.Timeout = TimeSpan.FromSeconds(30);
        o.CircuitBreaker = new CircuitBreakerOptions
        {
            FailureRatio      = 0.5,
            MinimumThroughput = 10,
            SamplingDuration  = TimeSpan.FromSeconds(30),
            BreakDuration     = TimeSpan.FromSeconds(30)
        };
    })
    .Build();
```

Invalid values throw from `AddConsumer`. Registering the same consumer again with other options throws. [NymBroker.ResilienceSample](../samples/NymBroker.ResilienceSample) runs both against a SQLite queue and checks the outcome: `dotnet run --project samples/NymBroker.ResilienceSample`.

### Timeout

`ConsumerOptions.Timeout` (`TimeSpan?`, default `null` = no limit, otherwise above zero and at most 1 day) limits one call of the consumer:

- When the time is up, the consumer's `CancellationToken` is cancelled **and the broker stops waiting at once**. A consumer that ignores its token cannot hold the listener (or one of its `MaxConcurrentCalls` slots). It keeps running in the background, and what it does afterwards is logged (an exception at `Warning`).
- The message is settled like any consumer failure ([Reliability](reliability.md#success-retry-dead-letter)): retried by the transport, or dead-lettered with reason **`ConsumerTimedOut`** when retry is off for the consumer or the source has no redelivery. `ProcessResult.Exception` is a `TimeoutException`.
- The broker's own stop is still a clean shutdown, not a timeout.
- Because a timed-out consumer may still finish its work, the message can be handled twice. Make the consumer idempotent ([duplicate detection](reliability.md#duplicate-detection)).

### Circuit breaker

`ConsumerOptions.CircuitBreaker` (`CircuitBreakerOptions?`, default `null`) gives the consumer one breaker, shared by all its message types:

- **Closed:** messages are consumed normally and each outcome is counted over a rolling `SamplingDuration`. When at least `MinimumThroughput` calls are in the window and the share of failures reaches `FailureRatio`, the circuit **opens**. A timeout counts as a failure. The broker's own stop is never counted.
- **Open:** for `BreakDuration` the consumer is not called. The broker **holds** the message (the listener waits) instead of failing it. A `Retry` would be redelivered at once and use up the transport's delivery count (`MaxRetryCount`, `MaxDeliveryCount`), so a downstream outage would dead-letter good messages. Holding is backpressure: no message is lost or counted against its retries.
- **Half-open:** after the break, one message is the trial. If it succeeds the circuit closes; if it fails the circuit opens for another `BreakDuration`. Other messages for the consumer wait until the trial is done.

Things to know about holding:

- Waiting messages occupy their listener. A SQL queue table with `MaxConcurrentCalls = 1` stops claiming while the circuit is open. That is the point, but other message types on the same endpoint wait too.
- A held message is still leased or locked by its transport. Keep `BreakDuration` below the lease or lock duration (the SQL queue lease, the Service Bus lock, the SQS visibility timeout, RabbitMQ's `consumer_timeout`), and below the idempotency store's claim lease if you use one. Otherwise the transport may hand the message to another listener while it is held here.
- Stopping the broker ends the wait at once; the message stays in the transport.
- The state is in `INymBroker.Describe()` (`ConsumerDescription.CircuitState`, read when called) and on the [dashboard](dashboard.md) topology page. It is not part of the health check: an open circuit means a downstream system is failing, and marking this app unhealthy would only get healthy instances restarted.

`CircuitBreakerOptions`:

| Option | Type | Default | Description |
|---|---|---|---|
| `FailureRatio` | `double` | `0.5` | Share of failed calls in the window that opens the circuit, in (0, 1]. |
| `MinimumThroughput` | `int` | `10` | Calls the window must hold before the ratio can open the circuit. At least 1. |
| `SamplingDuration` | `TimeSpan` | `30 s` | Length of the rolling window (10 buckets). At least 10 ms. |
| `BreakDuration` | `TimeSpan` | `30 s` | How long the circuit stays open before a trial. Above zero, at most 1 day. |
| `ShouldHandle` | `Func<Exception, bool>` | every exception | Which exceptions count as failures; others are not counted. A cancellation the call raised itself (e.g. an `HttpClient` timeout) counts; the caller's own cancellation never does. |
| `TimeProvider` | `TimeProvider` | `TimeProvider.System` | Clock for the window and the break. Replace it in tests. |

## `CircuitBreaker`

Use the breaker on its own to protect any call. Build one per protected resource and reuse it; it is thread-safe:

```csharp
var breaker = new CircuitBreaker(new CircuitBreakerOptions { MinimumThroughput = 5, BreakDuration = TimeSpan.FromSeconds(15) });

try
{
    var rate = await breaker.ExecuteAsync(async token => await rates.GetAsync(currency, token), ct);
}
catch (BrokenCircuitException ex)
{
    // Not called: the circuit is open. ex.RetryAfter is the time until the next trial (null while a trial runs).
}
```

Unlike the consumer option, `ExecuteAsync` **fails fast** while the circuit is open: it throws `BrokenCircuitException` without calling the action. `State` reads the current `CircuitState` (`Closed`, `Open`, `HalfOpen`). Exceptions from the action propagate unchanged. To combine it with `RetryPolicy`, call the breaker inside the retry, so each attempt is counted.

## Rate and concurrency limits

Limit how fast and how much in parallel the broker calls a downstream system: a rate-limited API, an SMS provider, or a database that copes with only a few writers. A limit is set per consumer or per destination endpoint. Both are off by default.

```csharp
using NymBroker.Core.Resilience;

services.AddNymBroker()
    .AddSqlServerEndPoint("Notifications", queueSettings)
    .AddHttpEndPoint("PartnerApi", httpSettings)
    // At most 5 calls per second and 2 at a time, across every endpoint that feeds this consumer.
    .AddConsumer<SmsConsumer>(o =>
    {
        o.RateLimit = new RateLimitOptions { PermitsPerPeriod = 5, Period = TimeSpan.FromSeconds(1) };
        o.ConcurrencyLimit = 2;
    })
    // Everything the broker sends to PartnerApi: posts, routes, topics, wire taps, dead letters, deferred and outbox sends.
    .LimitSendsTo("PartnerApi", o =>
    {
        o.RateLimit = new RateLimitOptions { PermitsPerPeriod = 50, Burst = 10 };
        o.ConcurrencyLimit = 4;
    })
    .Build();
```

**A call over the limit waits; it never fails.** If the broker answered `Retry` instead, the message would be redelivered at once and use up its delivery count, so a burst would dead-letter good messages. Waiting is backpressure:

- A consumer limit holds the message on its listener, so the endpoint takes no more work while it waits. This works on every transport, push ones included: each listener waits for the broker before it takes the next message.
- A send limit makes `PostAsync` / `PostBatchAsync` return later (the caller's token ends the wait). A route or topic forward holds its source message.
- Stopping the broker ends every wait at once; the message stays in its transport. A wait is never counted by the [circuit breaker](#circuit-breaker), and a consumer's [timeout](#timeout) counts only the call, not the wait.

Behaviour:

- **Order of the checks for a consumer:** circuit breaker → concurrency limit → rate limit → the call within its timeout. While the circuit is open, no rate permits are used.
- **The rate limit** is a token bucket: `PermitsPerPeriod` calls per `Period`, spread evenly, and up to `Burst` calls at once after a quiet time (default `Burst` = `PermitsPerPeriod`; use `Burst = 1` for an even pace). Waiting callers are served in the order they arrived. A batch send takes one permit per message, so `PostBatchAsync` of 100 messages at 10 per second waits about 10 s and then sends the batch at once.
- **The concurrency limit** counts calls that run at the same time. For a consumer it is shared by all its message types and all endpoints that feed it, on top of the endpoints' own `MaxConcurrentCalls` / `PrefetchCount`. A batch send counts as one. A consumer call that passed its timeout and keeps running in the background no longer counts.
- **Per process.** A limit of 50 per second on 3 instances is 150 per second overall. Divide by the number of instances, or put the limit where it is shared (for example in the downstream API's gateway).
- Invalid values throw from `AddConsumer` / `LimitSendsTo`. `LimitSendsTo` once per endpoint; `Build()` throws for an endpoint that is not registered. `Describe()` shows the limits (`ConsumerDescription.RateLimit` / `ConcurrencyLimit`, `EndpointDescription.SendRateLimit` / `SendConcurrencyLimit`), and so does the [dashboard](dashboard.md).

**Keep waits within the transport's lease or lock.** A message that waits is still leased or locked by its transport. The whole batch or prefetch has to get through the limit before that time is up, or the transport hands the messages to another listener:

| Transport | What must fit | Roughly |
|---|---|---|
| SQLite, PostgreSQL, SQL Server, MySQL | the claimed batch, within `LeaseTimeout` (5 min) | `BatchSize` / rate < `LeaseTimeout` |
| RabbitMQ | the unacked prefetch, within the server's `consumer_timeout` (30 min) | `PrefetchCount` / rate < `consumer_timeout` |
| Azure Service Bus | each message within its lock; handled messages are renewed up to `MaxAutoLockRenewalDuration` (5 min), prefetched ones are not | `MaxConcurrentCalls` / rate < `MaxAutoLockRenewalDuration`; keep `PrefetchCount` low |
| Amazon SQS | the receive batch, within the visibility timeout | `MaxNumberOfMessages` / rate < visibility timeout |
| Memory, File | nothing: one message at a time, no lease | — |

With the [idempotent receiver](reliability.md#duplicate-detection) the claim lease runs while a message waits too.

The limiter itself is internal; only `RateLimitOptions` and `SendLimitOptions` are public. Use `PostDeferredAsync` instead when you know *when* the downstream is free again ([Deferring a message](sending-messages.md#deferring-a-message)).

## Resilience metrics

All on the `NymBroker.Resilience` meter ([Observability](observability.md#metrics)): `nymbroker.retries`, `nymbroker.circuit_breaker.state_changes` (tags `circuit`, `state`), `nymbroker.circuit_breaker.held` (messages the broker held, tag `circuit`), `nymbroker.circuit_breaker.rejected` (calls `ExecuteAsync` refused, tag `circuit`) `nymbroker.consumer.timeouts` (tag `consumer`), `nymbroker.throttle.waits` (calls that waited for a limit, tags `scope` = `consumer`/`endpoint`, `name`, `limit` = `rate`/`concurrency`) and `nymbroker.throttle.wait.duration` (ms, tags `scope`, `name`; only calls that waited). A consumer's circuit is named after the consumer class. A standalone `CircuitBreaker` is named `default`. State changes are also logged (opened at `Warning`, half-open and closed at `Information`).
