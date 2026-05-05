using System.Diagnostics;
using MessageBroker.Benchmarks;
using MessageBroker.Core.DI;
using MessageBroker.Core.Impl;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

const int MemoryCount = 50_000;
const int FileCount   = 100;
var Settings = Path.Combine(AppContext.BaseDirectory, "benchmarksettings.json");

Console.OutputEncoding = System.Text.Encoding.UTF8;
Console.WriteLine("MessageBroker Throughput Benchmarks");
Console.WriteLine(new string('=', 42));
Console.WriteLine($"  Memory scenarios : {MemoryCount:N0} messages");
Console.WriteLine($"  File scenario    : {FileCount:N0} messages");
Console.WriteLine();

Console.Write("Warming up... ");
await RunAsync("_warmup", "Memory", 1_000);
Console.WriteLine("done");
Console.WriteLine();

Console.Write("Watcher sanity check... ");
await WatcherSanityCheckAsync();

Console.WriteLine("Running scenarios:");
var results = new List<BenchmarkResult>
{
    await RunAsync("Memory – direct",   "Memory",   MemoryCount),
    await RunAsync("Memory – routed",   "Memory",   MemoryCount,
        configure: broker => broker.Route<BenchmarkMessage>()
            .To("MemoryRouted").WhenFrom("Memory").Build()),
    await RunAsync("Memory – filtered", "Memory",   MemoryCount, addFilter: true),
    await RunAsync("File   – direct",   "FileLoop", FileCount,   fileDir: "bench-in"),
};

PrintTable(results);

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
    Action<IMessageBroker>? configure = null,
    bool addFilter = false,
    string? fileDir = null)
{
    var isWarmup = name.StartsWith('_');
    if (!isWarmup) Console.Write($"  {name,-22} ... ");

    if (fileDir != null)
        CleanDir(fileDir);

    var services = new ServiceCollection();
    services.AddLogging(b => b.SetMinimumLevel(LogLevel.Error));
    services.AddSingleton<CompletionTracker>();

    var builder = services.AddMessageBroker()
        .LoadConfiguration(Settings)
        .AddConsumer<BenchmarkConsumer>();
    builder.Build();

    await using var provider = services.BuildServiceProvider();
    var broker  = provider.GetRequiredService<IMessageBroker>();
    var tracker = provider.GetRequiredService<CompletionTracker>();

    configure?.Invoke(broker);
    if (addFilter)
        broker.AddFilter(new PassthroughFilter());

    await broker.StartAsync();

    // Clean GC baseline before measuring.
    GC.Collect(2, GCCollectionMode.Forced, blocking: true);
    GC.WaitForPendingFinalizers();
    GC.Collect(2, GCCollectionMode.Forced, blocking: true);

    var gen0Before  = GC.CollectionCount(0);
    var gen1Before  = GC.CollectionCount(1);
    var gen2Before  = GC.CollectionCount(2);
    var allocBefore = GC.GetTotalAllocatedBytes(precise: true);

    var completion = tracker.Prepare(count);
    var sw         = Stopwatch.StartNew();

    var payload = new string('X', 32);
    for (var i = 0; i < count; i++)
        await broker.PostAsync(endpoint, new BenchmarkMessage(i, payload));

    try { await completion.WaitAsync(TimeSpan.FromSeconds(30)); }
    catch (TimeoutException) { Console.WriteLine($"\n  [TIMEOUT] processed {tracker.Processed}/{count}"); }
    sw.Stop();

    var gen0      = GC.CollectionCount(0) - gen0Before;
    var gen1      = GC.CollectionCount(1) - gen1Before;
    var gen2      = GC.CollectionCount(2) - gen2Before;
    var allocated = (long)(GC.GetTotalAllocatedBytes(precise: true) - allocBefore);

    await broker.StopAsync();

    if (fileDir != null)
        CleanDir(fileDir);

    if (!isWarmup) Console.WriteLine("done");

    return new BenchmarkResult(name, count, sw.ElapsedMilliseconds, gen0, gen1, gen2, allocated);
}

// ─── output ─────────────────────────────────────────────────────────────────

void PrintTable(List<BenchmarkResult> rows)
{
    const string fmt = "{0,-25} {1,11} {2,11} {3,6} {4,6} {5,6} {6,13}";
    Console.WriteLine();
    Console.WriteLine(string.Format(fmt,
        "Scenario", "Msg/sec", "Elapsed", "Gen0", "Gen1", "Gen2", "Alloc/msg"));
    Console.WriteLine(new string('─', 83));
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
}

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
