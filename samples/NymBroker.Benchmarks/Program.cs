using System.Diagnostics;
using System.Net.Http.Headers;
using NymBroker.Benchmarks;
using NymBroker.Core.DI;
using NymBroker.Core.Factory;
using NymBroker.Core.Impl;
using NymBroker.Core.Route;
using NymBroker.Endpoint.Sqlite;
using NymBroker.Endpoint.Postgres;
using NymBroker.Endpoint.RabbitMq;
using NymBroker.Endpoint.SqlServer;
using NymBroker.Endpoint.AzureServiceBus;
using NymBroker.Endpoint.AwsSqs;
using NymBroker.Endpoint.MySql;
using MySqlConnector;
using Amazon.Runtime;
using Amazon.SQS;
using Azure.Messaging.ServiceBus;
using Npgsql;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

const int    MemoryCount  = 50_000;
const int    FileCount    = 100;
const int    SqlCount     = 1_000;
const int    PgCount      = 1_000;
const int    MssqlCount   = 1_000;
const int    SbCount      = 1_000;
const int    RabbitCount  = 1_000;
const int    SqsCount     = 1_000;
const int    MySqlCount   = 1_000;
const int    SplitCount   = 200;
const int    SplitThresholdBytes = 16 * 1024;
// In-process scenarios finish in about a second, so one run is mostly noise and JIT warm-up. They get their own
// warm-up pass, then several measured runs; the median run is reported.
const int    InProcessWarmup = 10_000;
const int    InProcessRuns   = 5;
const string PgConnStr    = "Host=localhost;Database=nymbroker;Username=postgres;Password=postgres";
// Service Bus emulator (scripts/setup-servicebus.ps1); AMQP on 5673 because RabbitMQ owns 5672.
const string SbConnStr    = "Endpoint=sb://localhost:5673;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
const string SbQueue      = "nymbroker.bench";
const string MssqlConnStr = "Server=localhost,1433;Database=nymbroker;User Id=sa;Password=NymBroker!Dev123;TrustServerCertificate=True";
const string MssqlTable   = "dbo.nymbroker_bench";
const string RabbitQueue  = "nymbroker.bench";
const string RabbitMgmt   = "http://localhost:15672";
// ElasticMQ (scripts/setup-sqs.ps1); the bench queue is created on first run.
const string SqsServiceUrl = "http://localhost:9324";
const string SqsQueue      = "nymbroker-bench";
const string MySqlConnStr  = "Server=localhost;Port=3306;Database=nymbroker;User ID=root;Password=NymBroker!Dev123";
const string MySqlTable    = "nymbroker_bench";
var Settings = Path.Combine(AppContext.BaseDirectory, "benchmarksettings.json");
// --latency: also collect the broker's own metrics (slows the run; see LatencyRecorder).
var Latency = args.Contains("--latency");

Console.OutputEncoding = System.Text.Encoding.UTF8;
Console.WriteLine("NymBroker Throughput Benchmarks");
Console.WriteLine(new string('=', 42));
Console.WriteLine($"  Memory scenarios : {MemoryCount:N0} messages, median of {InProcessRuns} runs after {InProcessWarmup:N0} warm-up messages");
Console.WriteLine($"  File scenario    : {FileCount:N0} messages");
Console.WriteLine($"  SQLite scenario  : {SqlCount:N0} messages");
Console.WriteLine($"  Postgres scenario: {PgCount:N0} messages");
Console.WriteLine($"  SQL Server scen. : {MssqlCount:N0} messages");
Console.WriteLine($"  Service Bus scen.: {SbCount:N0} messages");
Console.WriteLine($"  RabbitMQ scenarios: {RabbitCount:N0} messages (direct + batch/100)");
Console.WriteLine($"  Split scenarios  : {SplitCount:N0} messages (threshold={SplitThresholdBytes / 1024} KB)");
if (Latency) Console.WriteLine("  --latency        : broker metrics collected; Msg/sec is lower than without it");
Console.WriteLine();

// Large, highly compressible payload — big enough to require several split parts at
// SplitThresholdBytes, and repetitive enough that Brotli compression noticeably shrinks it.
var splitPayload = string.Concat(Enumerable.Repeat("The quick brown fox jumps over the lazy dog. ", 6_000));

Console.Write("Warming up... ");
// A full in-process scenario, not a token run: the first scenarios measured otherwise ran up to 2× slower than the
// same scenario later in the process (GC budgets, thread pool and shared JIT code still settling).
await RunAsync("_warmup", "Memory", MemoryCount, runs: InProcessRuns, warmup: InProcessWarmup);
Console.WriteLine("done");
Console.WriteLine();

Console.Write("Watcher sanity check... ");
await WatcherSanityCheckAsync();

Console.WriteLine("Running scenarios:");
var results = new List<BenchmarkResult>
{
    await RunAsync("Memory – direct",      "Memory",  MemoryCount, runs: InProcessRuns, warmup: InProcessWarmup),
    await RunAsync("Memory – routed",      "Memory",  MemoryCount, runs: InProcessRuns, warmup: InProcessWarmup,
        configure: broker => broker.Route<BenchmarkMessage>()
            .To("MemoryRouted").WhenFrom("Memory").Build()),
    await RunAsync("Memory – filtered",    "Memory",  MemoryCount, runs: InProcessRuns, warmup: InProcessWarmup, addFilter: true),

    await RunAsync("PubSub – 1 sub",      "Memory",  MemoryCount, runs: InProcessRuns, warmup: InProcessWarmup, usePubSub: true,
        configureBuilder: b =>
        {
            b.AddTopic<BenchmarkMessage>("bench.pubsub.1")
                .SubscribeWith<BenchmarkSubscriberA>()
                .Build();
        }),

    await RunAsync("PubSub – 3 subs",     "Memory",  MemoryCount, runs: InProcessRuns, warmup: InProcessWarmup, usePubSub: true,
        signalsPerMessage: 3,
        configureBuilder: b =>
        {
            b.AddTopic<BenchmarkMessage>("bench.pubsub.3")
                .SubscribeWith<BenchmarkSubscriberA>()
                .SubscribeWith<BenchmarkSubscriberB>()
                .SubscribeWith<BenchmarkSubscriberC>()
                .Build();
        }),

    // Topic fans out to PubSubDest endpoint; consumer on that endpoint signals.
    // WhenNotFrom prevents re-matching when the copy arrives at the destination endpoint.
    await RunAsync("PubSub – endpoint",   "Memory",  MemoryCount, runs: InProcessRuns, warmup: InProcessWarmup, usePubSub: true,
        configureBuilder: b =>
        {
            b.AddTopic<BenchmarkMessage>("bench.pubsub.ep")
                .When(new NotFromRouteCondition("PubSubDest"))
                .SubscribeTo("PubSubDest")
                .Build();
        }),

    await RunAsync("File   – direct",     "FileLoop", FileCount, fileDir: "bench-in"),

    await RunAsync("SQLite – direct",     "SqlBench", SqlCount,
        configureBuilder: b => b.AddSqliteEndPoint("SqlBench", new SqliteSettings
        {
            ConnectionString = "Data Source=:memory:",
            TableName        = "BenchMessages",
            BatchSize        = 100,
            PollInterval     = TimeSpan.Zero
        })),

    await RunAsync("Split+Compress – direct", "Memory", SplitCount,
        messageFactory: i => new BenchmarkMessage(i, splitPayload),
        splitThresholdBytes: SplitThresholdBytes, compress: true),

    await RunAsync("Split – no compress",     "Memory", SplitCount,
        messageFactory: i => new BenchmarkMessage(i, splitPayload),
        splitThresholdBytes: SplitThresholdBytes, compress: false),
};

if (await IsPostgresAvailableAsync())
{
    await CleanPgTableAsync();
    var pgSettings = new PostgresSettings
    {
        ConnectionString = PgConnStr,
        TableName        = "nymbroker_bench",
        BatchSize        = 50,
        AutoCreateTable  = true,
        PollInterval     = TimeSpan.Zero
    };
    results.Add(await RunAsync("Postgres – direct", "PgBench", PgCount,
        configureBuilder: b => b.AddPostgresEndPoint("PgBench", pgSettings)));
    await CleanPgTableAsync();
    results.Add(await RunAsync("Postgres – batch/100", "PgBench", PgCount, postBatchSize: 100,
        configureBuilder: b => b.AddPostgresEndPoint("PgBench", pgSettings)));
}
else
{
    Console.WriteLine("  Postgres – direct        ... skipped (Postgres not available)");
}

if (await IsSqlServerAvailableAsync())
{
    await CleanSqlServerTableAsync();
    var mssqlSettings = new SqlServerSettings
    {
        ConnectionString = MssqlConnStr,
        TableName        = MssqlTable,
        BatchSize        = 50,
        AutoCreateTable  = true,
        PollInterval     = TimeSpan.Zero
    };
    results.Add(await RunAsync("SqlServer – direct", "MssqlBench", MssqlCount,
        configureBuilder: b => b.AddSqlServerEndPoint("MssqlBench", mssqlSettings)));
    await CleanSqlServerTableAsync();
    results.Add(await RunAsync("SqlServer – batch/100", "MssqlBench", MssqlCount, postBatchSize: 100,
        configureBuilder: b => b.AddSqlServerEndPoint("MssqlBench", mssqlSettings)));
}
else
{
    Console.WriteLine("  SqlServer – direct       ... skipped (SQL Server not available)");
}

if (await IsMySqlAvailableAsync())
{
    await CleanMySqlTableAsync();
    var mySqlSettings = new MySqlSettings
    {
        ConnectionString = MySqlConnStr,
        TableName        = MySqlTable,
        BatchSize        = 50,
        AutoCreateTable  = true,
        PollInterval     = TimeSpan.Zero
    };
    results.Add(await RunAsync("MySQL – direct", "MySqlBench", MySqlCount,
        configureBuilder: b => b.AddMySqlEndPoint("MySqlBench", mySqlSettings)));
    await CleanMySqlTableAsync();
    results.Add(await RunAsync("MySQL – batch/100", "MySqlBench", MySqlCount, postBatchSize: 100,
        configureBuilder: b => b.AddMySqlEndPoint("MySqlBench", mySqlSettings)));
}
else
{
    Console.WriteLine("  MySQL – direct           ... skipped (MySQL not available)");
}

if (await IsServiceBusAvailableAsync())
{
    await DrainServiceBusQueueAsync();
    var sbSettings = new AzureServiceBusSettings
    {
        ConnectionString = SbConnStr,
        QueueName        = SbQueue,
        PrefetchCount    = 100
    };
    results.Add(await RunAsync("ServiceBus – direct", "SbBench", SbCount,
        configureBuilder: b => b.AddAzureServiceBusEndPoint("SbBench", sbSettings)));
    await DrainServiceBusQueueAsync();
    results.Add(await RunAsync("ServiceBus – batch/100", "SbBench", SbCount, postBatchSize: 100,
        configureBuilder: b => b.AddAzureServiceBusEndPoint("SbBench", sbSettings)));
}
else
{
    Console.WriteLine("  ServiceBus – direct      ... skipped (Service Bus emulator or queue not available)");
}

if (await IsRabbitMqAvailableAsync())
{
    await PurgeRabbitQueueAsync();
    results.Add(await RunAsync("RabbitMQ – direct",     "RabbitBench", RabbitCount,
        configureBuilder: b => b.AddRabbitMqEndPoint("RabbitBench", new RabbitMqSettings
        {
            HostName       = "localhost",
            ReadQueueName  = RabbitQueue,
            WriteQueueName = RabbitQueue,
            BatchAckSize   = 1,
        })));
    await PurgeRabbitQueueAsync();
    results.Add(await RunAsync("RabbitMQ – batch/100", "RabbitBench", RabbitCount,
        configureBuilder: b => b.AddRabbitMqEndPoint("RabbitBench", new RabbitMqSettings
        {
            HostName       = "localhost",
            ReadQueueName  = RabbitQueue,
            WriteQueueName = RabbitQueue,
            BatchAckSize   = 100,
        })));
}
else
{
    Console.WriteLine("  RabbitMQ – direct        ... skipped (RabbitMQ not available)");
}

if (await TryCreateSqsQueueAsync() is { } sqsQueueUrl)
{
    var sqsSettings = new AwsSqsSettings
    {
        QueueUrl        = sqsQueueUrl,
        ServiceUrl      = SqsServiceUrl,
        Credentials     = new BasicAWSCredentials("local", "local"),
        WaitTimeSeconds = 1
    };
    await PurgeSqsQueueAsync(sqsQueueUrl);
    results.Add(await RunAsync("SQS – direct", "SqsBench", SqsCount,
        configureBuilder: b => b.AddAwsSqsEndPoint("SqsBench", sqsSettings)));
    await PurgeSqsQueueAsync(sqsQueueUrl);
    results.Add(await RunAsync("SQS – batch/100", "SqsBench", SqsCount, postBatchSize: 100,
        configureBuilder: b => b.AddAwsSqsEndPoint("SqsBench", sqsSettings)));
}
else
{
    Console.WriteLine("  SQS – direct             ... skipped (ElasticMQ not available)");
}

PrintTable(results);

// ─── sqs helpers ─────────────────────────────────────────────────────────────

AmazonSQSClient CreateSqsClient()
    => new(new BasicAWSCredentials("local", "local"), new AmazonSQSConfig { ServiceURL = SqsServiceUrl, AuthenticationRegion = "us-east-1", MaxErrorRetry = 0 });

async Task<string?> TryCreateSqsQueueAsync()
{
    try
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        using var client = CreateSqsClient();
        return (await client.CreateQueueAsync(SqsQueue, cts.Token)).QueueUrl;   // returns the existing queue's URL
    }
    catch { return null; }
}

async Task PurgeSqsQueueAsync(string queueUrl)
{
    using var client = CreateSqsClient();
    await client.PurgeQueueAsync(queueUrl);
}

// ─── postgres helpers ────────────────────────────────────────────────────────

async Task<bool> IsPostgresAvailableAsync()
{
    try
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        await using var ds   = NpgsqlDataSource.Create(PgConnStr);
        await using var conn = await ds.OpenConnectionAsync(cts.Token);
        return true;
    }
    catch { return false; }
}

async Task CleanPgTableAsync()
{
    await using var ds   = NpgsqlDataSource.Create(PgConnStr);
    await using var conn = await ds.OpenConnectionAsync();
    await using var cmd  = conn.CreateCommand();
    cmd.CommandText = "DROP TABLE IF EXISTS nymbroker_bench";
    await cmd.ExecuteNonQueryAsync();
}

// ─── mysql helpers ───────────────────────────────────────────────────────────

async Task<bool> IsMySqlAvailableAsync()
{
    try
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        await using var conn = new MySqlConnection(MySqlConnStr);
        await conn.OpenAsync(cts.Token);
        return true;
    }
    catch { return false; }
}

async Task CleanMySqlTableAsync()
{
    await using var conn = new MySqlConnection(MySqlConnStr);
    await conn.OpenAsync();
    await using var cmd = new MySqlCommand($"DROP TABLE IF EXISTS {MySqlTable}", conn);
    await cmd.ExecuteNonQueryAsync();
}

// ─── sql server helpers ──────────────────────────────────────────────────────

async Task<bool> IsSqlServerAvailableAsync()
{
    try
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        await using var conn = new SqlConnection(MssqlConnStr);
        await conn.OpenAsync(cts.Token);
        return true;
    }
    catch { return false; }
}

async Task CleanSqlServerTableAsync()
{
    await using var conn = new SqlConnection(MssqlConnStr);
    await conn.OpenAsync();
    await using var cmd = conn.CreateCommand();
    cmd.CommandText = $"DROP TABLE IF EXISTS {MssqlTable}";
    await cmd.ExecuteNonQueryAsync();
}

// ─── service bus helpers ─────────────────────────────────────────────────────

async Task<bool> IsServiceBusAvailableAsync()
{
    try
    {
        // Peeking proves both the emulator and the bench queue (scripts/servicebus/Config.json) exist.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await using var client = new ServiceBusClient(SbConnStr);
        await using var receiver = client.CreateReceiver(SbQueue);
        await receiver.PeekMessageAsync(cancellationToken: cts.Token);
        return true;
    }
    catch { return false; }
}

async Task DrainServiceBusQueueAsync()
{
    await using var client = new ServiceBusClient(SbConnStr);
    foreach (var subQueue in new[] { SubQueue.None, SubQueue.DeadLetter })
    {
        await using var receiver = client.CreateReceiver(SbQueue, new ServiceBusReceiverOptions { SubQueue = subQueue });
        IReadOnlyList<ServiceBusReceivedMessage> batch;
        while ((batch = await receiver.ReceiveMessagesAsync(200, TimeSpan.FromMilliseconds(500))).Count > 0)
            foreach (var m in batch) await receiver.CompleteMessageAsync(m);
    }
}

// ─── rabbitmq helpers ───────────────────────────────────────────────────────

async Task<bool> IsRabbitMqAvailableAsync()
{
    try
    {
        using var cts  = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        using var http = new HttpClient();
        http.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Basic",
                Convert.ToBase64String("guest:guest"u8.ToArray()));
        var resp = await http.GetAsync($"{RabbitMgmt}/api/overview", cts.Token);
        return resp.IsSuccessStatusCode;
    }
    catch { return false; }
}

async Task PurgeRabbitQueueAsync()
{
    try
    {
        using var http = new HttpClient();
        http.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Basic",
                Convert.ToBase64String("guest:guest"u8.ToArray()));
        await http.DeleteAsync($"{RabbitMgmt}/api/queues/%2F/{RabbitQueue}/contents");
    }
    catch { }
}

// ─── watcher sanity ──────────────────────────────────────────────────────────

async Task WatcherSanityCheckAsync()
{
    const int n = 5;
    var dir = Path.Combine(AppContext.BaseDirectory, "bench-sanity");
    Directory.CreateDirectory(dir);

    var seen = 0;
    var tcs  = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    using var w = new FileSystemWatcher(dir, "*.json")
    {
        NotifyFilter        = NotifyFilters.FileName,
        InternalBufferSize  = 65536,
        EnableRaisingEvents = true
    };
    w.Created += (_, _) => { if (Interlocked.Increment(ref seen) >= n) tcs.TrySetResult(); };

    for (var i = 0; i < n; i++)
    {
        var path = Path.Combine(dir, $"s{i}_{Guid.NewGuid():N}.json");
        await using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 4096, true);
        await fs.WriteAsync("{\"ok\":1}"u8.ToArray());
    }

    var ok = await Task.WhenAny(tcs.Task, Task.Delay(5_000)) == tcs.Task;
    Console.WriteLine(ok ? $"OK ({n}/{n})" : $"FAIL ({seen}/{n} events received)");

    foreach (var f in Directory.GetFiles(dir)) try { File.Delete(f); } catch { }
    try { Directory.Delete(dir); } catch { }
}

// ─── scenario runner ────────────────────────────────────────────────────────

async Task<BenchmarkResult> RunAsync(
    string name,
    string endpoint,
    int count,
    Action<INymBroker>? configure = null,
    Action<NymBrokerBuilder>? configureBuilder = null,
    bool addFilter = false,
    bool usePubSub = false,
    int signalsPerMessage = 1,
    string? fileDir = null,
    Func<int, BenchmarkMessage>? messageFactory = null,
    int? splitThresholdBytes = null,
    bool compress = true,
    int? postBatchSize = null,
    int runs = 1,
    int warmup = 0)
{
    var isWarmup = name.StartsWith('_');
    if (!isWarmup) Console.Write($"  {name,-24} ... ");

    if (fileDir != null)
        CleanDir(fileDir);

    var services = new ServiceCollection();
    services.AddLogging(b => b.SetMinimumLevel(LogLevel.Error));
    services.AddSingleton<CompletionTracker>();

    var builder = services.AddNymBroker()
        .LoadConfiguration(Settings)
        .AddConsumer<BenchmarkConsumer>();

    configureBuilder?.Invoke(builder);
    builder.Build();

    await using var provider = services.BuildServiceProvider();
    var broker  = provider.GetRequiredService<INymBroker>();
    var tracker = provider.GetRequiredService<CompletionTracker>();

    configure?.Invoke(broker);
    if (addFilter)
        broker.AddFilter(new PassthroughFilter());

    await broker.StartAsync();

    // Warm up this scenario's own code path (filter, routing, topics…) and give tiered JIT time to install the
    // optimized code, so the measured runs don't include compilation.
    if (warmup > 0)
    {
        await MeasureAsync(warmup, collectLatency: false);
        await Task.Delay(500);
    }

    var measuredRuns = new List<BenchmarkResult>(runs);
    for (var run = 0; run < runs; run++)
        measuredRuns.Add(await MeasureAsync(count, collectLatency: Latency && !isWarmup));
    var result = measuredRuns.OrderBy(r => r.ElapsedMs).ElementAt(runs / 2);   // median run

    await broker.StopAsync();

    if (fileDir != null)
        CleanDir(fileDir);

    if (!isWarmup) Console.WriteLine("done");

    return result;

    async Task<BenchmarkResult> MeasureAsync(int messages, bool collectLatency)
    {
        // Clean GC baseline before measuring.
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);

        var gen0Before  = GC.CollectionCount(0);
        var gen1Before  = GC.CollectionCount(1);
        var gen2Before  = GC.CollectionCount(2);
        var allocBefore = GC.GetTotalAllocatedBytes(precise: true);

        using var recorder = collectLatency ? new LatencyRecorder() : null;
        var completion = tracker.Prepare(messages * signalsPerMessage);
        var sw         = Stopwatch.StartNew();

        var payload = new string('X', 32);
        var pending = new List<BenchmarkMessage>(postBatchSize ?? 0);
        for (var i = 0; i < messages; i++)
        {
            var msg = messageFactory != null ? messageFactory(i) : new BenchmarkMessage(i, payload);
            if (usePubSub)
                await broker.PublishAsync(msg);
            else if (postBatchSize.HasValue)
            {
                pending.Add(msg);
                if (pending.Count == postBatchSize.Value)
                {
                    await broker.PostBatchAsync(endpoint, pending, splitThresholdBytes: splitThresholdBytes, compress: compress);
                    pending.Clear();
                }
            }
            else
                await broker.PostAsync(endpoint, msg, splitThresholdBytes: splitThresholdBytes, compress: compress);
        }
        if (pending.Count > 0)
            await broker.PostBatchAsync(endpoint, pending, splitThresholdBytes: splitThresholdBytes, compress: compress);

        try { await completion.WaitAsync(TimeSpan.FromSeconds(30)); }
        catch (TimeoutException)
        {
            Console.WriteLine($"\n  [TIMEOUT] processed {tracker.Processed}/{messages * signalsPerMessage}");
        }
        sw.Stop();

        var gen0      = GC.CollectionCount(0) - gen0Before;
        var gen1      = GC.CollectionCount(1) - gen1Before;
        var gen2      = GC.CollectionCount(2) - gen2Before;
        var allocated = (long)(GC.GetTotalAllocatedBytes(precise: true) - allocBefore);
        return new BenchmarkResult(name, messages, sw.ElapsedMilliseconds, gen0, gen1, gen2, allocated, recorder?.Stop());
    }
}

// ─── output ─────────────────────────────────────────────────────────────────

void PrintTable(List<BenchmarkResult> rows)
{
    const string fmt = "{0,-27} {1,11} {2,11} {3,6} {4,6} {5,6} {6,13}";
    Console.WriteLine();
    Console.WriteLine(string.Format(fmt,
        "Scenario", "Msg/sec", "Elapsed", "Gen0", "Gen1", "Gen2", "Alloc/msg"));
    Console.WriteLine(new string('─', 86));
    foreach (var r in rows)
    {
        var msgsPerSec  = r.ElapsedMs > 0 ? (long)(r.MessageCount * 1000.0 / r.ElapsedMs) : 0;
        var allocPerMsg = r.MessageCount > 0 ? r.AllocatedBytes / r.MessageCount : 0;
        Console.WriteLine(string.Format(fmt,
            r.Name,
            $"{msgsPerSec:N0}",
            $"{r.ElapsedMs:N0} ms",
            r.Gen0, r.Gen1, r.Gen2,
            $"{FormatBytes(allocPerMsg)}/msg"));
    }
    Console.WriteLine();

    if (!Latency) return;
    const string lfmt = "{0,-27} {1,11} {2,11} {3,11} {4,8} {5,8}";
    Console.WriteLine(string.Format(lfmt, "Scenario", "Proc p50", "Proc p99", "Age p99", "Failed", "Retries"));
    Console.WriteLine(new string('─', 86));
    foreach (var r in rows.Where(r => r.Latency != null))
    {
        var l = r.Latency!;
        Console.WriteLine(string.Format(lfmt, r.Name,
            FormatMs(l.ProcessingP50Ms), FormatMs(l.ProcessingP99Ms), FormatMs(l.AgeP99Ms), l.Failed, l.Retries));
    }
    Console.WriteLine();
}

string FormatMs(double ms) => double.IsNaN(ms) ? "–" : ms < 10 ? $"{ms:F3} ms" : $"{ms:N0} ms";

string FormatBytes(long bytes) => bytes switch
{
    >= 1_048_576 => $"{bytes / 1_048_576.0:F1} MB",
    >= 1_024     => $"{bytes / 1_024.0:F1} KB",
    _            => $"{bytes} B"
};

void CleanDir(string relativePath)
{
    var full = Path.Combine(AppContext.BaseDirectory, relativePath);
    if (!Directory.Exists(full)) return;
    foreach (var f in Directory.GetFiles(full))
        try { File.Delete(f); } catch { /* best-effort on leftover locked files */ }
}
