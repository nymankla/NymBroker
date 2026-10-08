---
name: nymbroker-testing
description: Write unit and integration tests for code that uses NymBroker — consumers, subscribers, routes, topics, filters, input transformers and failure/retry paths — using Memory and in-memory SQLite endpoints and a reliable completion signal instead of Task.Delay. Use when the user wants to test a NymBroker consumer, message flow, route, retry or dead-letter behaviour, or has flaky messaging tests.
---

# Test NymBroker code

Message handling is asynchronous: a test posts, the broker's listener picks the message up on another thread. Tests must **wait for a signal** with a timeout, never sleep a fixed time.

Docs: https://github.com/nymankla/NymBroker/blob/master/docs/messages-and-consumers.md

## 1. Pick the level

| What | How |
|---|---|
| Consumer / subscriber logic | Unit test: call `ConsumeAsync` / `ReceiveAsync` directly with a test `IMessageContext` |
| Input transformer | Unit test: call `Transform(bytes, "Endpoint")` and assert the `RawMessageContext` |
| Registration, routes, topics, filters, type names | Integration test: real broker, `AddMemoryEndPoint` |
| Retries and dead-lettering | Integration test: `AddSqliteEndPoint` with `Data Source=:memory:` (Memory endpoints never retry) |
| A specific transport (PostgreSQL, SQL Server, RabbitMQ, Service Bus) | Integration test against a container (Testcontainers / docker compose), skipped when not available |

Follow the test framework and conventions already in the solution (xUnit, NUnit, MSTest; assertion library). No extra NymBroker test package is needed.

## 2. Unit test a consumer

```csharp
using NymBroker.Core.Message;

sealed class TestContext : IMessageContext
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public Guid CorrelationId { get; set; } = Guid.NewGuid();
    public EndpointAddress? Address { get; set; } = EndpointAddress.Create("Orders", "Orders");
    public string? MessageType { get; set; } = "orders.order-created";
    public DateTime Created { get; set; } = DateTime.UtcNow;
}

[Fact]
public async Task Stores_new_order()
{
    var repo = new FakeOrderRepository();
    var consumer = new OrderCreatedConsumer(repo, NullLogger<OrderCreatedConsumer>.Instance);

    await consumer.ConsumeAsync(new OrderCreated("ORD-1", "C-1", 10m, DateTimeOffset.UtcNow), new TestContext());

    Assert.Single(repo.Orders);
}
```

Also test that a redelivered message (same `Id`, same order) is harmless, and that invalid input throws or is ignored as intended.

## 3. Integration test with a completion signal

Register a small probe the consumer (or a test double) signals, and wait on it:

```csharp
public sealed class Probe<T>
{
    private readonly TaskCompletionSource<T> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public void Set(T value) => _tcs.TrySetResult(value);
    public Task<T> WaitAsync(TimeSpan? timeout = null) => _tcs.Task.WaitAsync(timeout ?? TimeSpan.FromSeconds(5));
}

sealed class CapturingConsumer(Probe<(OrderCreated Message, IMessageContext Context)> probe) : IConsume<OrderCreated>
{
    public Task ConsumeAsync(OrderCreated message, IMessageContext context, CancellationToken ct = default)
    {
        probe.Set((message, context));
        return Task.CompletedTask;
    }
}

[Fact]
public async Task Order_reaches_consumer()
{
    var services = new ServiceCollection().AddLogging();
    var probe = new Probe<(OrderCreated, IMessageContext)>();
    services.AddSingleton(probe);
    services.AddNymBroker()
        .AddMemoryEndPoint("Orders")
        .AddConsumer<CapturingConsumer>()
        .Build();

    await using var provider = services.BuildServiceProvider();
    var broker = provider.GetRequiredService<INymBroker>();
    await broker.StartAsync();
    try
    {
        await broker.PostAsync("Orders", new OrderCreated("ORD-1", "C-1", 10m, DateTimeOffset.UtcNow));
        var (message, context) = await probe.WaitAsync();
        Assert.Equal("ORD-1", message.OrderId);
        Assert.Equal("Orders", context.Address?.From);
    }
    finally { await broker.StopAsync(); }
}
```

- Call `broker.StartAsync()` / `StopAsync()` yourself (no host is running in the test), or build a real `Host` and start it.
- To test the **real** consumer, register it as normal and replace its dependencies with fakes that signal a probe.
- To count N messages, use a counter with `Interlocked.Decrement` that completes the probe at zero.
- To prove something did **not** happen (filtered, routed away), post a second "sentinel" message that must arrive, wait for it, then assert the first didn't — no fixed delays.
- Routes: register Memory endpoints for source and destination, add the route, put a capturing consumer on the destination, and assert `context.Address.From` is the destination.
- Keep one broker per test (fresh `ServiceCollection`); brokers are cheap to build.

## 4. Retries and dead letters

Use in-memory SQLite, which retries and marks rows Failed:

```csharp
services.AddNymBroker()
    .AddSqliteEndPoint("Orders", new SqliteSettings
    {
        ConnectionString = "Data Source=:memory:",
        TableName        = "Orders",
        AutoCreateTable  = true,
        MaxRetryCount    = 3,
        PollInterval     = TimeSpan.FromMilliseconds(10)
    })
    .AddConsumer<AlwaysFailingConsumer>()   // counts calls, throws, signals the probe on the 3rd call
    .Build();
```

Assert the consumer was called exactly `MaxRetryCount` times. For the broker dead-letter path (Memory/File or `UseNativeDeadLetter = false`), point `WithDeadLetterEndpoint` at a second broker's endpoint, or at a SQLite/File endpoint you read afterwards, and check `context.DeadLetter.Reason == DeadLetterReasons.ConsumerFailed`. Don't consume the dead-letter endpoint with the same consumer type in the same broker — it would loop.

## 5. Finish

Run the tests several times (`dotnet test` with a repeat, or in a loop) to make sure they aren't flaky, and tell the user what is covered and what still needs a real transport.
