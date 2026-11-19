using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NymBroker.Core;
using NymBroker.Core.Consume;
using NymBroker.Core.DI;
using NymBroker.Core.Message;
using NymBroker.Core.Resilience;
using NymBroker.Endpoint.Sqlite;

// A consumer timeout and a circuit breaker on two SQLite queues (#134). Self-checking: exits 0 when every
// expectation holds, 1 otherwise.
//
//   dotnet run --project samples/NymBroker.ResilienceSample
//
//  1. Timeout:         the report service hangs on its first call and ignores the token. After 1 s the broker stops
//                      waiting, the queue retries the message, and the second attempt succeeds.
//  2. Circuit breaker: the payment gateway is down for 3 s while 10 payments arrive. After 3 failures the circuit
//                      opens and the broker holds the other payments instead of failing them. They are not counted
//                      against MaxRetryCount = 3, so none is dead-lettered. Once the gateway is back, a trial payment
//                      closes the circuit and every payment goes through.

var folder = Directory.CreateTempSubdirectory("nymbroker-resilience-");
var db = Path.Combine(folder.FullName, "resilience.db");

SqliteSettings Queue(string table) => new()
{
    ConnectionString = $"Data Source={db}",
    TableName = table,
    PollInterval = TimeSpan.FromMilliseconds(50),
    MaxRetryCount = 3                              // without the breaker, 3 failed attempts dead-letter a payment
};

var gateway = new PaymentGateway(downFor: TimeSpan.FromSeconds(3));
var reports = new ReportService();

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.SetMinimumLevel(LogLevel.Warning)
    .AddFilter("NymBroker.Core.Impl.NymBrokerImpl", LogLevel.Information)   // consumer failures and circuit state changes
    .AddFilter("NymBroker.Endpoint", LogLevel.None);                          // the queue's own retry log lines
builder.Logging.AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss.fff "; });
builder.Services.AddSingleton(gateway).AddSingleton(reports);

builder.Services.AddNymBroker()
    .AddSqliteEndPoint("Reports", Queue("Reports"))
    .AddSqliteEndPoint("Payments", Queue("Payments"))
    // Timeout only: a hung call no longer holds the Reports listener.
    .AddConsumer<ReportConsumer>(o => o.Timeout = TimeSpan.FromSeconds(1))
    // Timeout and circuit breaker: opens when half of at least 3 calls in 10 s fail, and tries again after 2 s.
    .AddConsumer<PaymentConsumer>(o =>
    {
        o.Timeout = TimeSpan.FromSeconds(5);
        o.CircuitBreaker = new CircuitBreakerOptions
        {
            FailureRatio = 0.5,
            MinimumThroughput = 3,
            SamplingDuration = TimeSpan.FromSeconds(10),
            BreakDuration = TimeSpan.FromSeconds(2),   // keep it below the queue's LeaseTimeout (5 min)
            ShouldHandle = static ex => ex is HttpRequestException or TimeoutException
        };
    })
    .Build();

using var host = builder.Build();
var broker = host.Services.GetRequiredService<INymBroker>();
await host.StartAsync();

var failures = new List<string>();
void Check(bool condition, string expectation)
{
    Console.WriteLine($"  {(condition ? "ok  " : "FAIL")} {expectation}");
    if (!condition) failures.Add(expectation);
}

// --- 1. Timeout ---
Console.WriteLine("1. Timeout: the report service hangs on the first call");
await broker.PostAsync("Reports", new ReportRequested("monthly-sales"));
await WaitUntilAsync(() => reports.Completed == 1, TimeSpan.FromSeconds(15));
var reportCounts = await broker.GetQueueInspector("Reports")!.GetCountsAsync();
Check(reports.Completed == 1, "the report was generated");
Check(reports.Calls == 2, $"it took 2 calls: the first timed out (calls: {reports.Calls})");
Check(reportCounts.Failed == 0, "nothing was dead-lettered");

// --- 2. Circuit breaker ---
Console.WriteLine();
Console.WriteLine("2. Circuit breaker: the payment gateway is down for 3 s");
gateway.GoDown();
for (var i = 1; i <= 10; i++)
    await broker.PostAsync("Payments", new PaymentRequested($"ORD-{i:000}", 10m * i));

await WaitUntilAsync(() => gateway.Charged == 10, TimeSpan.FromSeconds(30));
var paymentCounts = await broker.GetQueueInspector("Payments")!.GetCountsAsync();
var circuit = broker.Describe().Consumers.Single(c => c.Consumer == nameof(PaymentConsumer)).CircuitState;
Check(gateway.Charged == 10, $"all 10 payments went through (charged: {gateway.Charged})");
Check(paymentCounts.Failed == 0, $"none was dead-lettered, although MaxRetryCount is 3 (failed: {paymentCounts.Failed})");
Check(gateway.CallsWhileDown <= 5, $"the gateway was called only {gateway.CallsWhileDown} times while down (without the breaker: up to 30)");
Check(circuit == CircuitState.Closed, $"the circuit is closed again ({circuit})");

await host.StopAsync();
reports.ReleaseHungCall();
host.Dispose();
SqliteCleanup();

Console.WriteLine();
Console.WriteLine(failures.Count == 0 ? "All checks passed." : $"{failures.Count} check(s) failed.");
return failures.Count == 0 ? 0 : 1;

static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
{
    var until = DateTime.UtcNow + timeout;
    while (!condition() && DateTime.UtcNow < until)
        await Task.Delay(50);
}

void SqliteCleanup()
{
    Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
    try { folder.Delete(recursive: true); }
    catch (IOException ex) { Console.WriteLine($"Could not delete {folder.FullName}: {ex.Message}"); }
}

[MessageName("sample.resilience.report-requested")]
public sealed record ReportRequested(string Name);

[MessageName("sample.resilience.payment-requested")]
public sealed record PaymentRequested(string OrderId, decimal Amount);

/// <summary>A downstream report service whose first call hangs and ignores its token.</summary>
public sealed class ReportService
{
    private readonly TaskCompletionSource _hung = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _calls;
    private int _completed;

    public int Calls => Volatile.Read(ref _calls);
    public int Completed => Volatile.Read(ref _completed);

    public async Task GenerateAsync(string name, CancellationToken ct)
    {
        if (Interlocked.Increment(ref _calls) == 1)
            await _hung.Task;               // a call that never returns and ignores ct
        else
            await Task.Delay(100, ct);
        Interlocked.Increment(ref _completed);
    }

    /// <summary>Ends the hung call (as cancelled), so nothing is left running when the process exits.</summary>
    public void ReleaseHungCall() => _hung.TrySetCanceled();
}

/// <summary>A payment gateway that answers 503 for a while after <see cref="GoDown"/>.</summary>
public sealed class PaymentGateway(TimeSpan downFor)
{
    private DateTime _downUntil = DateTime.MinValue;
    private int _charged;
    private int _callsWhileDown;

    public int Charged => Volatile.Read(ref _charged);
    public int CallsWhileDown => Volatile.Read(ref _callsWhileDown);

    public void GoDown() => _downUntil = DateTime.UtcNow + downFor;

    public async Task ChargeAsync(string orderId, decimal amount, CancellationToken ct)
    {
        await Task.Delay(20, ct);
        if (DateTime.UtcNow < _downUntil)
        {
            Interlocked.Increment(ref _callsWhileDown);
            throw new HttpRequestException($"503 Service Unavailable (charging {orderId})");
        }
        Interlocked.Increment(ref _charged);
    }
}

public sealed class ReportConsumer(ReportService reports, ILogger<ReportConsumer> logger) : IConsume<ReportRequested>
{
    public async Task ConsumeAsync(ReportRequested message, IMessageContext context, CancellationToken ct = default)
    {
        await reports.GenerateAsync(message.Name, ct);   // always pass the token on
        logger.LogWarning("Report '{Name}' generated", message.Name);
    }
}

public sealed class PaymentConsumer(PaymentGateway gateway) : IConsume<PaymentRequested>
{
    public Task ConsumeAsync(PaymentRequested message, IMessageContext context, CancellationToken ct = default)
        => gateway.ChargeAsync(message.OrderId, message.Amount, ct);
}
