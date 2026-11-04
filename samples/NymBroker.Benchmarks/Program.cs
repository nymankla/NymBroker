using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text;
using System.Security.Cryptography;
using NymBroker.Benchmarks;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Factory;
using NymBroker.Core;
using NymBroker.Core.Route;
using NymBroker.Endpoint.Sqlite;
using NymBroker.Endpoint.Postgres;
using NymBroker.Endpoint.RabbitMq;
using NymBroker.Endpoint.SqlServer;
using NymBroker.Endpoint.AzureServiceBus;
using NymBroker.Endpoint.AwsSqs;
using NymBroker.Endpoint.MySql;
using NymBroker.Endpoint.Http;
using NymBroker.Idempotency.Postgres;
using NymBroker.Idempotency.Sqlite;
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
const int    SbCount      = 200;      // the emulator limits this to about 35 messages a second
const int    RabbitCount  = 1_000;
const int    SqsCount     = 1_000;
const int    MySqlCount   = 1_000;
const int    SplitCount   = 200;
const int    SplitThresholdBytes = 16 * 1024;
const int    BacklogFactor = 5;       // a drain scenario reads this many times the transport's message count
const int    SlowPathCount = 10_000;  // scenarios that do far more per message than the fast in-process ones
const int    LargeCount    = 5_000;
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
const string PgIdempotencyTable = "nymbroker_bench_idempotency";
var Settings = Path.Combine(AppContext.BaseDirectory, "benchmarksettings.json");
// --latency: also collect the broker's own metrics (slows the run; see LatencyRecorder).
var Latency = args.Contains("--latency");
// --filter <text>: run only the scenarios whose name contains the text.
var filterAt = Array.IndexOf(args, "--filter");
var Filter = filterAt >= 0 && filterAt + 1 < args.Length ? args[filterAt + 1] : null;

Console.OutputEncoding = System.Text.Encoding.UTF8;
Console.WriteLine("NymBroker Throughput Benchmarks");
Console.WriteLine(new string('=', 42));
Console.WriteLine($"  In-process scenarios: median of {InProcessRuns} runs, each after its own warm-up");
Console.WriteLine($"  Transport scenarios: one run, skipped if the service is not reachable");
if (Filter != null) Console.WriteLine($"  --filter           : only scenarios containing '{Filter}'");
if (Latency) Console.WriteLine("  --latency        : broker metrics collected; Msg/sec is lower than without it");
Console.WriteLine();

// Large, highly compressible payload — big enough to require several split parts at
// SplitThresholdBytes, and repetitive enough that Brotli compression noticeably shrinks it.
var splitPayload = string.Concat(Enumerable.Repeat("The quick brown fox jumps over the lazy dog. ", 6_000));
// As large, but about as hard to compress as text gets: Base64 of random bytes.
var randomPayload = Convert.ToBase64String(RandomNumberGenerator.GetBytes(200_000));
var largePayload = new string('X', 64 * 1024);

Console.Write("Warming up... ");
// A full in-process scenario, not a token run: the first scenarios measured otherwise ran up to 2× slower than the
// same scenario later in the process (GC budgets, thread pool and shared JIT code still settling).
await RunAsync("_warmup", "Memory", MemoryCount, runs: InProcessRuns, warmup: InProcessWarmup);
Console.WriteLine("done");
Console.WriteLine();

Console.WriteLine("Running scenarios:");
var results = new List<BenchmarkResult?>
{
    // ── The pipeline ─────────────────────────────────────────────────────────────────────────────────────
    await RunAsync("Memory – direct",      "Memory",  MemoryCount, runs: InProcessRuns, warmup: InProcessWarmup),
    await RunAsync("Memory – 8 producers", "Memory",  MemoryCount, runs: InProcessRuns, warmup: InProcessWarmup, producers: 8),
    await RunAsync("Memory – batch/100",   "Memory",  MemoryCount, runs: InProcessRuns, warmup: InProcessWarmup, postBatchSize: 100),
    await RunAsync("Memory – 64 KB",       "Memory",  LargeCount,  runs: InProcessRuns, warmup: LargeCount,
        messageFactory: i => new BenchmarkMessage(i, largePayload)),
    await RunAsync("Memory – routed",      "Memory",  MemoryCount, runs: InProcessRuns, warmup: InProcessWarmup,
        configureBuilder: b => b.AddRoute<BenchmarkMessage>(r => r.To("MemoryRouted").WhenFrom("Memory"))),
    // 19 routes read the message and do not match; the 20th sends it on.
    await RunAsync("Memory – 20 routes",   "Memory",  MemoryCount, runs: InProcessRuns, warmup: InProcessWarmup,
        configureBuilder: b =>
        {
            for (var i = 0; i < 19; i++)
                b.AddRoute<BenchmarkMessage>(r => r.To("MemoryRouted").When(m => m.GetProperty("seq").GetInt32() < 0).WhenFrom("Memory"));
            b.AddRoute<BenchmarkMessage>(r => r.To("MemoryRouted").WhenFrom("Memory"));
        }),
    await RunAsync("Memory – wire tap",    "Memory",  MemoryCount, runs: InProcessRuns, warmup: InProcessWarmup,
        configureBuilder: b => AddSink(b, "Tap", signal: false).AddWireTap("Tap")),
    await RunAsync("Input transformer",    "Memory",  MemoryCount, runs: InProcessRuns, warmup: InProcessWarmup, bareInput: true,
        configureBuilder: b => b.AddInputTransformer<BareJsonInputTransformer>("Memory")),
    await RunAsync("Output transformer",   "Memory",  MemoryCount, runs: InProcessRuns, warmup: InProcessWarmup,
        configureBuilder: b =>
        {
            AddSink(b, "Partner", signal: true).AddOutputTransformer<BareJsonOutputTransformer>("Partner");
            b.AddRoute<BenchmarkMessage>(r => r.To("Partner").WhenFrom("Memory"));
        }),

    // ── Topics ───────────────────────────────────────────────────────────────────────────────────────────
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

    // ── Reliability ──────────────────────────────────────────────────────────────────────────────────────
    await RunAsync("Idempotent – memory", "Memory",  MemoryCount, runs: InProcessRuns, warmup: InProcessWarmup,
        configureBuilder: b => b.AddIdempotentReceiver()),
    await RunAsync("Idempotent – SQLite", "Memory",  SqlCount,
        configureBuilder: b => b.AddSqliteIdempotency(new SqliteIdempotencySettings { ConnectionString = "Data Source=:memory:", CleanupInterval = TimeSpan.Zero })),
    // Every consumer call throws, and the broker dead-letters the message: what a run of poison messages costs.
    await RunAsync("Consumer fails → DLQ", "Memory", SlowPathCount, runs: InProcessRuns, warmup: SlowPathCount, consumerFails: true,
        configureBuilder: b => AddSink(b, "Dlq", signal: true).WithDeadLetterEndpoint("Dlq")),
    // Due in a second, so Msg/sec mostly shows that wait; Alloc/msg is the cost of holding a message in the broker.
    await RunAsync("Deferred – in broker", "Plain", 20_000, deferBy: TimeSpan.FromSeconds(1),
        configureBuilder: b =>
        {
            b.Services.AddKeyedSingleton<IEndPoint>("Plain", (_, _) => new NonDeferringEndPoint("Plain"));
            b.RegisterEndpoint("Plain");
        }),

    // ── Large messages ───────────────────────────────────────────────────────────────────────────────────
    await RunAsync("Split+Compress – direct", "Memory", SplitCount,
        messageFactory: i => new BenchmarkMessage(i, splitPayload),
        splitThresholdBytes: SplitThresholdBytes, compress: true),

    await RunAsync("Split – no compress",     "Memory", SplitCount,
        messageFactory: i => new BenchmarkMessage(i, splitPayload),
        splitThresholdBytes: SplitThresholdBytes, compress: false),

    await RunAsync("Split+Compress – random", "Memory", SplitCount,
        messageFactory: i => new BenchmarkMessage(i, randomPayload),
        splitThresholdBytes: SplitThresholdBytes, compress: true),

    // ── Transports that need no service ──────────────────────────────────────────────────────────────────
    await RunAsync("File   – direct",     "FileLoop", FileCount, fileDir: "bench-in"),

    // No network and no server: what the HTTP endpoint and HttpClient add per request.
    await RunAsync("HTTP – no network",   "HttpBench", SlowPathCount, runs: InProcessRuns, warmup: SlowPathCount,
        configureBuilder: b => b.AddHttpEndPoint("HttpBench", new HttpSettings { Url = "http://localhost/bench", MaxRetryAttempts = 0 },
            client => client.ConfigurePrimaryHttpMessageHandler(sp => new CountingHandler(sp.GetRequiredService<CompletionTracker>())))),
};

var sqliteSettings = new SqliteSettings
{
    ConnectionString = "Data Source=:memory:",
    TableName        = "BenchMessages",
    BatchSize        = 100,
    PollInterval     = TimeSpan.Zero
};
results.Add(await RunAsync("SQLite – direct", "SqlBench", SqlCount,
    configureBuilder: b => b.AddSqliteEndPoint("SqlBench", sqliteSettings)));
results.Add(await RunAsync("SQLite – batch/100", "SqlBench", SqlCount, postBatchSize: 100,
    configureBuilder: b => b.AddSqliteEndPoint("SqlBench", sqliteSettings)));
results.Add(await RunAsync("SQLite – drain backlog", "SqlBench", SqlCount * BacklogFactor, drainBacklog: true,
    configureBuilder: b => b.AddSqliteEndPoint("SqlBench", sqliteSettings)));
// A time in the past: every message goes through the deferred table and is moved to the queue at once.
results.Add(await RunAsync("SQLite – deferred", "SqlBench", SqlCount, deferBy: TimeSpan.FromMinutes(-1),
    configureBuilder: b => b.AddSqliteEndPoint("SqlBench", sqliteSettings)));

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
    await CleanPgTableAsync();
    results.Add(await RunAsync("Postgres – drain backlog", "PgBench", PgCount * BacklogFactor, drainBacklog: true,
        configureBuilder: b => b.AddPostgresEndPoint("PgBench", pgSettings)));
    await CleanPgTableAsync();
    results.Add(await RunAsync("Postgres – deferred", "PgBench", PgCount, deferBy: TimeSpan.FromMinutes(-1),
        configureBuilder: b => b.AddPostgresEndPoint("PgBench", pgSettings)));
    await CleanPgTableAsync();
    results.Add(await RunAsync("Idempotent – Postgres", "Memory", PgCount,
        configureBuilder: b => b.AddPostgresIdempotency(new PostgresIdempotencySettings
        {
            ConnectionString = PgConnStr,
            TableName        = PgIdempotencyTable,
            CleanupInterval  = TimeSpan.Zero
        })));
    await CleanPgTableAsync();
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
    await CleanSqlServerTableAsync();
    results.Add(await RunAsync("SqlServer – drain backlog", "MssqlBench", MssqlCount * BacklogFactor, drainBacklog: true,
        configureBuilder: b => b.AddSqlServerEndPoint("MssqlBench", mssqlSettings)));
    await CleanSqlServerTableAsync();
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
    await CleanMySqlTableAsync();
    results.Add(await RunAsync("MySQL – drain backlog", "MySqlBench", MySqlCount * BacklogFactor, drainBacklog: true,
        configureBuilder: b => b.AddMySqlEndPoint("MySqlBench", mySqlSettings)));
    await CleanMySqlTableAsync();
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
}
else
{
    Console.WriteLine("  ServiceBus – direct      ... skipped (Service Bus emulator or queue not available)");
}

if (await IsRabbitMqAvailableAsync())
{
    var rabbitSettings = new RabbitMqSettings { HostName = "localhost", ReadQueueName = RabbitQueue, WriteQueueName = RabbitQueue };
    await PurgeRabbitQueueAsync();
    results.Add(await RunAsync("RabbitMQ – direct", "RabbitBench", RabbitCount,
        configureBuilder: b => b.AddRabbitMqEndPoint("RabbitBench", rabbitSettings)));
    await PurgeRabbitQueueAsync();
    results.Add(await RunAsync("RabbitMQ – batch/100", "RabbitBench", RabbitCount, postBatchSize: 100,
        configureBuilder: b => b.AddRabbitMqEndPoint("RabbitBench", new RabbitMqSettings
        {
            HostName       = rabbitSettings.HostName,
            ReadQueueName  = RabbitQueue,
            WriteQueueName = RabbitQueue,
            BatchAckSize   = 100
        })));
    await PurgeRabbitQueueAsync();
    results.Add(await RunAsync("RabbitMQ – not durable", "RabbitBench", RabbitCount,
        configureBuilder: b => b.AddRabbitMqEndPoint("RabbitBench", new RabbitMqSettings
        {
            HostName       = rabbitSettings.HostName,
            ReadQueueName  = RabbitQueue,
            WriteQueueName = RabbitQueue,
            DurablePublish = false
        })));
    await PurgeRabbitQueueAsync();
    results.Add(await RunAsync("RabbitMQ – drain backlog", "RabbitBench", RabbitCount * BacklogFactor, drainBacklog: true,
        configureBuilder: b => b.AddRabbitMqEndPoint("RabbitBench", rabbitSettings)));
    await PurgeRabbitQueueAsync();
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
    await PurgeSqsQueueAsync(sqsQueueUrl);
    results.Add(await RunAsync("SQS – drain backlog", "SqsBench", SqsCount * BacklogFactor, drainBacklog: true,
        configureBuilder: b => b.AddAwsSqsEndPoint("SqsBench", sqsSettings)));
}
else
{
    Console.WriteLine("  SQS – direct             ... skipped (ElasticMQ not available)");
}

PrintTable(results.OfType<BenchmarkResult>().ToList());

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
    cmd.CommandText = $"DROP TABLE IF EXISTS nymbroker_bench, nymbroker_bench_deferred, {PgIdempotencyTable}";
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
    await using var cmd = new MySqlCommand($"DROP TABLE IF EXISTS {MySqlTable}, {MySqlTable}_deferred", conn);
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
    cmd.CommandText = $"DROP TABLE IF EXISTS {MssqlTable}, {MssqlTable}_deferred";
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

// ─── scenario runner ────────────────────────────────────────────────────────

NymBrokerBuilder AddSink(NymBrokerBuilder builder, string name, bool signal)
{
    builder.Services.AddKeyedSingleton<IEndPoint>(name, (sp, _) => new SinkEndPoint(sp.GetRequiredService<CompletionTracker>(), signal));
    builder.RegisterEndpoint(name);
    return builder;
}

async Task<BenchmarkResult?> RunAsync(
    string name,
    string endpoint,
    int count,
    Action<NymBrokerBuilder>? configureBuilder = null,
    bool usePubSub = false,
    int signalsPerMessage = 1,
    string? fileDir = null,
    Func<int, BenchmarkMessage>? messageFactory = null,
    int? splitThresholdBytes = null,
    bool compress = true,
    int? postBatchSize = null,
    int runs = 1,
    int warmup = 0,
    int producers = 1,
    bool bareInput = false,
    bool consumerFails = false,
    TimeSpan? deferBy = null,
    bool drainBacklog = false)
{
    var isWarmup = name.StartsWith('_');
    if (!isWarmup && Filter != null && !name.Contains(Filter, StringComparison.OrdinalIgnoreCase))
        return null;
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
    tracker.ConsumerFails = consumerFails;

    // A drain scenario starts the broker itself, once the backlog is there.
    if (!drainBacklog)
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
        if (drainBacklog)
            await PostAllAsync(messages, batchSize: 100);

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

        if (drainBacklog)
            await broker.StartAsync();
        else if (producers > 1)
            await Task.WhenAll(Enumerable.Range(0, producers).Select(_ => Task.Run(() => PostAllAsync(messages / producers, postBatchSize))));
        else
            await PostAllAsync(messages, postBatchSize);

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

    async Task PostAllAsync(int messages, int? batchSize)
    {
        var payload = new string('X', 32);
        var pending = new List<BenchmarkMessage>(batchSize ?? 0);
        for (var i = 0; i < messages; i++)
        {
            var msg = messageFactory != null ? messageFactory(i) : new BenchmarkMessage(i, payload);
            if (bareInput)
                await broker.PostRawAsync(endpoint, Encoding.UTF8.GetBytes($$"""{"seq":{{i}},"data":"{{payload}}"}""").AsMemory());
            else if (deferBy is { } delay)
                await broker.PostDeferredAsync(endpoint, msg, DateTimeOffset.UtcNow + delay);
            else if (usePubSub)
                await broker.PublishAsync(msg);
            else if (batchSize.HasValue)
            {
                pending.Add(msg);
                if (pending.Count == batchSize.Value)
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
