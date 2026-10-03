# NymBroker.Resilience

A small retry policy with no package dependencies. It replaced Polly in NymBroker and is used by:

- `FileEndPoint`, to retry file reads that fail with `IOException` while a writer still has the file open or hasn't flushed it.
- `RabbitMqEndPoint`, to reconnect forever when the broker connection drops.

It only retries. There is no circuit breaker, timeout, fallback or hedging.

## Quick start

```csharp
using NymBroker.Resilience;

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

## `RetryOptions`

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

### `RetryAttempt` (passed to `OnRetry`)

| Property | Description |
|---|---|
| `AttemptNumber` | 0-based number of the attempt that just failed. The first retry has `AttemptNumber = 0`, so log `AttemptNumber + 1` to show a 1-based count. |
| `Exception` | The exception that caused this retry. |
| `Delay` | How long the policy will wait before the next attempt. |

## Backoff and jitter

`attempt` below is the 0-based `AttemptNumber`.

| `BackoffType` | `UseJitter = false` | `UseJitter = true` |
|---|---|---|
| `Constant` | `Delay` | `Delay` ± 25 % (uniformly random) |
| `Exponential` | `Delay × 2^attempt` (25, 50, 100, 200 ms … for a 25 ms base) | Decorrelated jitter: grows exponentially like the column to the left, but each delay is spread randomly |

Every delay is clamped to `MaxDelay` (24 h when unset). The exponential calculation can't overflow even with `MaxRetryAttempts = int.MaxValue`; very large attempt numbers simply resolve to `MaxDelay`.

The jitter formulas follow the ones Polly v8 uses (± 25 % for constant, "decorrelated jitter backoff V2" for exponential), so timing is close to what the endpoints had before.

## Execution rules

- The first attempt runs immediately. After a failure, the policy retries only if all of these hold:
  1. retries remain (`attempt < MaxRetryAttempts`),
  2. the `CancellationToken` has not been cancelled,
  3. `ShouldHandle(exception)` returns `true`.
- If any condition fails, the exception propagates as is: same instance, original stack trace, no wrapping in `AggregateException`.
- On each retry the order is: compute delay → `OnRetry` → wait → next attempt.
- Cancelling during the wait throws `OperationCanceledException` and nothing further runs.
- An exception thrown by `OnRetry` propagates and stops the retries.

## How NymBroker uses it

| Endpoint | Settings | Behaviour |
|---|---|---|
| `FileEndPoint` | `MaxRetryAttempts = 4`, `Delay = 25 ms`, `Exponential`, `UseJitter = true`, `ShouldHandle = ex is IOException`; logs at `Debug` | Retries a file that is still locked or empty when the watcher event fires. After the last attempt the endpoint logs a warning and skips the file, which keeps its original name and is not renamed to `.processed`. |
| `RabbitMqEndPoint` | `MaxRetryAttempts = int.MaxValue`, `Delay = RabbitMqSettings.ReconnectDelaySeconds`, `Constant`, default `ShouldHandle`; logs at `Warning` | Keeps reconnecting and re-subscribing until the listener is cancelled. |

## Testing

Use `Delay = TimeSpan.Zero` so tests don't wait, or pass a fake `TimeProvider` (for example `FakeTimeProvider` from `Microsoft.Extensions.TimeProvider.Testing`) to step through delays yourself. See `NymBroker.Tests/RetryPolicyTests.cs` for examples.
