using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NymBroker.Core.DI;
using NymBroker.Core;
using NymBroker.Core.Message;
using NymBroker.Core.Scheduling;
using NymBroker.Core.Serialize;
using NymBroker.Endpoint.RabbitMq;
using NymBroker.Idempotency.SqlServer;
using NymBroker.RabbitSqlServerSample;
using NymBroker.Scheduling.SqlServer;

// Messages travel through RabbitMQ; duplicate detection and the schedule are in SQL Server.
// Workers share the queue and both tables, so they can run on any number of machines.
//
// Needs: ./scripts/setup-rabbitmq.ps1 and ./scripts/setup-sqlserver.ps1
//
//   dotnet run                  # one worker until Ctrl+C; start it in two terminals to see them share the work
//   dotnet run -- --demo        # two workers in this process; checks the result, exit code 0 when it holds

const string ConnectionString = "Server=localhost,1433;Database=nymbroker;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True";
var scheduleInterval = TimeSpan.FromSeconds(5);

Console.OutputEncoding = System.Text.Encoding.UTF8;

try
{
    if (args.Contains("--demo")) return await RunDemoAsync();

    using var host = BuildWorker($"worker-{Environment.ProcessId}", new Recorder());
    Console.WriteLine("Worker running. A scheduled invoice is posted every 5 seconds, by one worker only. Ctrl+C to stop.");
    await host.RunAsync();
    return 0;
}
catch (Exception ex) when (ex is not OperationCanceledException)
{
    Console.Error.WriteLine($"Failed: {ex.Message}");
    Console.Error.WriteLine("Are RabbitMQ and SQL Server running? ./scripts/setup-rabbitmq.ps1 and ./scripts/setup-sqlserver.ps1");
    return 1;
}

IHost BuildWorker(string name, Recorder recorder)
{
    var builder = Host.CreateApplicationBuilder();
    builder.Logging.SetMinimumLevel(LogLevel.Warning);
    builder.Services.AddSingleton(recorder);
    builder.Services.AddSingleton(new WorkerName(name));

    builder.Services.AddNymBroker()
        // The transport. Workers compete for the messages on this queue.
        .AddRabbitMqEndPoint("Invoices", new RabbitMqSettings
        {
            HostName       = "localhost",
            ReadQueueName  = "nymbroker.sample.invoices",
            WriteQueueName = "nymbroker.sample.invoices"
        })
        .AddConsumer<InvoiceConsumer>()

        // A message id seen before is not handled again, on any worker, also after a restart.
        .AddSqlServerIdempotency(new SqlServerIdempotencySettings
        {
            ConnectionString = ConnectionString,
            TableName        = "dbo.nymbroker_sample_idempotency",
            Ttl              = TimeSpan.FromHours(1)
        })

        // Each occurrence of a named action runs on one worker. A run missed while all were down is caught up on start.
        .AddSqlServerScheduleStore(new SqlServerScheduleStoreSettings
        {
            ConnectionString = ConnectionString,
            TableName        = "dbo.nymbroker_sample_schedule"
        })
        .AddScheduledAction("invoice-run", scheduleInterval, (services, ct) =>
        {
            var now = DateTime.UtcNow;
            services.GetRequiredService<Recorder>().ScheduledRun(new DateTime(now.Ticks - now.Ticks % scheduleInterval.Ticks, DateTimeKind.Utc));
            Console.WriteLine($"  [{name}] scheduled run at {now:HH:mm:ss}");
            return services.GetRequiredService<INymBroker>().PostAsync("Invoices", new Invoice($"SCHEDULED-{Guid.NewGuid():N}"[..20]), ct);
        }, o => o.MissedRuns = MissedRunPolicy.RunOnce)
        .Build();

    return builder.Build();
}

async Task<int> RunDemoAsync()
{
    var failures = new List<string>();
    void Check(bool ok, string what)
    {
        Console.WriteLine($"  {(ok ? "OK  " : "FAIL")} {what}");
        if (!ok) failures.Add(what);
    }

    var recorder = new Recorder();
    var a = BuildWorker("A", recorder);
    var b = BuildWorker("B", recorder);
    await Task.WhenAll(a.StartAsync(), b.StartAsync());
    var broker = a.Services.GetRequiredService<INymBroker>();

    Console.WriteLine("1. Two workers share a schedule: each occurrence runs on one of them.");
    await WaitUntilAsync(() => recorder.ScheduledRuns.Count >= 3, TimeSpan.FromSeconds(30));
    var runs = recorder.ScheduledRuns.ToList();
    Check(runs.Count >= 3 && runs.Distinct().Count() == runs.Count, $"{runs.Count} runs in {runs.Distinct().Count()} different occurrences");

    Console.WriteLine("2. The same message posted twice is handled once.");
    var duplicate = $"DUPLICATE-{Guid.NewGuid():N}"[..20];
    var envelope = Envelope(new Invoice(duplicate));
    await broker.PostRawAsync("Invoices", envelope.AsMemory());
    await broker.PostRawAsync("Invoices", envelope.AsMemory());
    await WaitUntilAsync(() => recorder.TimesHandled(duplicate) > 0, TimeSpan.FromSeconds(15));
    await Task.Delay(TimeSpan.FromSeconds(2));
    Check(recorder.TimesHandled(duplicate) == 1, $"{duplicate} handled {recorder.TimesHandled(duplicate)} time(s)");

    Console.WriteLine("3. A failure is not remembered as done: the redelivery runs, and it is handled once.");
    var failing = $"FAILS-ONCE-{Guid.NewGuid():N}"[..20];
    await broker.PostAsync("Invoices", new Invoice(failing, FailFirstAttempt: true));
    await WaitUntilAsync(() => recorder.TimesHandled(failing) > 0, TimeSpan.FromSeconds(15));
    Check(recorder.Attempts(failing) == 2 && recorder.TimesHandled(failing) == 1,
        $"{failing}: {recorder.Attempts(failing)} attempts, handled {recorder.TimesHandled(failing)} time(s)");

    Console.WriteLine("4. All workers stop, and a run is missed. Both tables survive the restart.");
    await Task.WhenAll(a.StopAsync(), b.StopAsync());
    a.Dispose();
    b.Dispose();
    await Task.Delay(scheduleInterval + TimeSpan.FromSeconds(1));

    // Just after an occurrence, so a run in the next few seconds can only be the catch-up.
    var nextOccurrence = scheduleInterval.Ticks - DateTime.UtcNow.Ticks % scheduleInterval.Ticks;
    await Task.Delay(TimeSpan.FromTicks(nextOccurrence) + TimeSpan.FromMilliseconds(300));
    var runsBefore = recorder.ScheduledRuns.Count;
    using var restarted = BuildWorker("A (restarted)", recorder);
    await restarted.StartAsync();
    var caughtUp = await WaitUntilAsync(() => recorder.ScheduledRuns.Count > runsBefore, TimeSpan.FromSeconds(3));
    Check(caughtUp, "the missed run was caught up as soon as the worker started");

    await restarted.Services.GetRequiredService<INymBroker>().PostRawAsync("Invoices", envelope.AsMemory());
    await Task.Delay(TimeSpan.FromSeconds(2));
    Check(recorder.TimesHandled(duplicate) == 1, $"{duplicate} is still recognised as a duplicate");

    await restarted.StopAsync();

    Console.WriteLine();
    Console.WriteLine(failures.Count == 0 ? "All checks passed." : $"{failures.Count} check(s) failed.");
    return failures.Count == 0 ? 0 : 1;
}

// A serialized envelope has a fixed id, as when a producer sends a message again because it never saw the first send succeed.
static byte[] Envelope(Invoice invoice)
{
    using var stream = new MessageSerializerJson().Serialize(new MessageContext<Invoice> { Message = invoice });
    using var copy = new MemoryStream();
    stream.CopyTo(copy);
    return copy.ToArray();
}

static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
{
    var deadline = DateTime.UtcNow + timeout;
    while (!condition())
    {
        if (DateTime.UtcNow > deadline) return false;
        await Task.Delay(50);
    }
    return true;
}
