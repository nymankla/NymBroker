using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Threading.Channels;
using NymBroker.Core.Endpoint.HealthCheck;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Resilience;

namespace NymBroker.Core.Endpoint.File;

public sealed class FileEndPoint : IEndPointEventDriven, IDeferredEndPoint
{
    private const string DeferredFilePrefix = "deferred-";
    private const string DeferredTimeFormat = "yyyyMMddHHmmssfffffff";   // whole ticks, so rounding can never make a file due early
    // Task.Delay rejects longer waits (about 49 days); the reader checks a file again after each step.
    private static readonly TimeSpan MaxDelayStep = TimeSpan.FromDays(1);
    // A handler that never returns must not keep the broker from stopping.
    private static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(30);

    // Files waiting for their time, each with one wake-up: the poll loop sees them again and must not add more.
    private readonly ConcurrentDictionary<string, byte> _waitingFiles = new(StringComparer.OrdinalIgnoreCase);
    private readonly FileSettings _settings;
    private readonly ILogger<FileEndPoint> _logger;
    private readonly DirectoryInfo _readDir;
    private readonly DirectoryInfo _postDir;
    private readonly RetryPolicy _fileReadyPolicy;
    private readonly string _name;
    private FileSystemWatcher? _watcher;
    // _stopping ends the reading, polling and waiting at once; _handlerCts only a handler that outlasts StopTimeout.
    private CancellationTokenSource? _stopping;
    private CancellationTokenSource? _handlerCts;
    private Task? _loop;
    private Task? _poll;

    // All file events (watcher + poll + startup scan) feed into this channel.
    // Single reader ensures only one Task processes a given file at a time,
    // eliminating the concurrent-rename race that caused lost signals.
    private Channel<FileInfo>? _fileChannel;

    public EndpointMode Mode { get; }

    public FileEndPoint(string name, FileSettings settings, ILogger<FileEndPoint> logger, EndpointMode mode = EndpointMode.ReadWrite)
    {
        _name = name;
        Mode = mode;
        _settings = settings;
        _logger = logger;

        var basePath = settings.IsAbsolutePath
            ? string.Empty
            : AppContext.BaseDirectory;

        _readDir = new DirectoryInfo(Path.Combine(basePath, settings.ReadPath));
        _postDir = new DirectoryInfo(Path.Combine(basePath, settings.PostPath));

        _readDir.Create();
        _postDir.Create();

        _fileReadyPolicy = new RetryPolicy(new RetryOptions
        {
            MaxRetryAttempts = 4,
            Delay = TimeSpan.FromMilliseconds(25),
            BackoffType = RetryBackoffType.Exponential,
            UseJitter = true,
            ShouldHandle = static ex => ex is IOException,
            OnRetry = args =>
            {
                _logger.LogDebug(
                    "Retrying file read after transient file access failure on attempt {Attempt}: {Error}",
                    args.AttemptNumber + 1,
                    args.Exception.Message);
                return ValueTask.CompletedTask;
            }
        });
    }

    public Task PostAsync(byte[] message, CancellationToken ct = default)
    {
        var fileName = $"{DateTime.UtcNow:yyyyMMddHHmmss}_{Guid.NewGuid():N}.json";
        var path = Path.Combine(_postDir.FullName, fileName);
        return System.IO.File.WriteAllBytesAsync(path, message, ct);
    }

    /// <summary>
    /// Writes the file now, with its delivery time in the name (<c>deferred-&lt;UTC time&gt;_&lt;guid&gt;.json</c>). A
    /// <see cref="FileEndPoint"/> reading the folder leaves the file alone until then, so it survives a restart of either
    /// side. Another program reading the folder has to do the same.
    /// </summary>
    public Task PostDeferredAsync(byte[] message, DateTimeOffset deliverAt, CancellationToken ct = default)
    {
        if (deliverAt <= DateTimeOffset.UtcNow) return PostAsync(message, ct);

        var time = deliverAt.UtcDateTime.ToString(DeferredTimeFormat, CultureInfo.InvariantCulture);
        var path = Path.Combine(_postDir.FullName, $"{DeferredFilePrefix}{time}_{Guid.NewGuid():N}.json");
        return System.IO.File.WriteAllBytesAsync(path, message, ct);
    }

    /// <summary>The time in a deferred file's name; null for any other file, which is due at once.</summary>
    private static DateTimeOffset? GetDeliverAt(FileInfo file)
    {
        if (!file.Name.StartsWith(DeferredFilePrefix, StringComparison.Ordinal)) return null;

        var separator = file.Name.IndexOf('_', DeferredFilePrefix.Length);
        if (separator < 0) return null;

        return DateTimeOffset.TryParseExact(file.Name.AsSpan(DeferredFilePrefix.Length, separator - DeferredFilePrefix.Length),
            DeferredTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var deliverAt)
            ? deliverAt
            : null;
    }

    private static bool IsDue(FileInfo file) => GetDeliverAt(file) is not { } deliverAt || deliverAt <= DateTimeOffset.UtcNow;

    // Queues the file again when it is due, also if PollInterval is off.
    private void WakeWhenDue(FileInfo file, CancellationToken stopping)
    {
        if (!_waitingFiles.TryAdd(file.FullName, 0)) return;

        _ = Task.Run(async () =>
        {
            try
            {
                try
                {
                    var wait = GetDeliverAt(file)!.Value - DateTimeOffset.UtcNow;
                    if (wait > TimeSpan.Zero)
                        await Task.Delay(wait < MaxDelayStep ? wait : MaxDelayStep, stopping);
                }
                finally
                {
                    _waitingFiles.TryRemove(file.FullName, out _);   // before the write: the reader may need a new wake-up
                }
                _fileChannel?.Writer.TryWrite(file);
            }
            catch (OperationCanceledException) when (stopping.IsCancellationRequested) { }   // read again at the next start
            catch (Exception ex) { _logger.LogError(ex, "Error waiting for deferred file '{File}'", file.Name); }
        }, CancellationToken.None);
    }

    /// <summary>Starts reading on the thread pool and returns.</summary>
    /// <exception cref="InvalidOperationException">The endpoint is already listening.</exception>
    public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct)
    {
        if (_loop is not null)
            throw new InvalidOperationException($"File endpoint '{_name}' is already listening.");

        // Linked, because the token passed in is the broker's startup token, which stopping does not cancel.
        _stopping = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _handlerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var stopping = _stopping.Token;
        var handlerToken = _handlerCts.Token;

        var channel = Channel.CreateUnbounded<FileInfo>(
            new UnboundedChannelOptions { SingleReader = true, AllowSynchronousContinuations = false });
        _fileChannel = channel;

        // Single reader loop — only this task reads from the channel, so files are processed one at
        // a time. Duplicate entries (from watcher + poll scan racing for the same file) are harmless:
        // ReadAndArchiveAsync returns null on the second attempt and no handler call is made.
        _loop = Task.Run(() => ProcessChannelAsync(channel.Reader, handler, stopping, handlerToken), CancellationToken.None);

        _watcher = new FileSystemWatcher(_readDir.FullName, _settings.SearchPattern)
        {
            NotifyFilter       = NotifyFilters.FileName,
            InternalBufferSize = 65536,
            EnableRaisingEvents = true
        };
        _watcher.Created += OnFileCreated;
        _watcher.Error   += OnWatcherError;

        // Queue any files already present when listening starts.
        QueueExistingFiles();

        // Periodic poll — catches files whose watcher events were dropped under burst load.
        if (_settings.PollInterval > TimeSpan.Zero)
            _poll = Task.Run(() => PollAsync(stopping), CancellationToken.None);

        return Task.CompletedTask;
    }

    private async Task PollAsync(CancellationToken stopping)
    {
        using var timer = new PeriodicTimer(_settings.PollInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(stopping))
                QueueExistingFiles();
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested) { }
        catch (Exception ex) { _logger.LogError(ex, "Error in poll loop for '{Dir}'", _readDir.FullName); }
    }

    private void QueueExistingFiles()
    {
        foreach (var file in _readDir.GetFiles(_settings.SearchPattern))
            _fileChannel?.Writer.TryWrite(file);
    }

    /// <summary>
    /// Waits for the file being handled, so no handler runs after this returns. Files not read yet stay in the folder for
    /// the next start. After 30 seconds the running handler is cancelled.
    /// </summary>
    public async Task StopListeningAsync()
    {
        if (_watcher != null)
        {
            _watcher.EnableRaisingEvents = false;
            _watcher.Dispose();
            _watcher = null;
        }
        _fileChannel?.Writer.TryComplete();
        if (_loop is null) return;

        await _stopping!.CancelAsync();
        try
        {
            await _loop.WaitAsync(StopTimeout);
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("File endpoint '{Name}' was still handling a file after {Timeout}; cancelling its handler", _name, StopTimeout);
            await _handlerCts!.CancelAsync();
            await _loop;
        }
        if (_poll is not null) await _poll;

        _loop = null;
        _poll = null;
        _stopping.Dispose();
        _stopping = null;
        _handlerCts!.Dispose();
        _handlerCts = null;
    }

    public async IAsyncEnumerable<string> ReadAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var file in _readDir.GetFiles(_settings.SearchPattern))
        {
            if (ct.IsCancellationRequested) yield break;
            if (!IsDue(file)) continue;
            var content = await ReadAndArchiveAsync(file, ct);
            if (content != null) yield return content;
        }
    }

    public IHealthCheckResult HealthCheck()
    {
        try
        {
            return _readDir.Exists && _postDir.Exists
                ? HealthCheckResult.Healthy()
                : HealthCheckResult.Unhealthy($"Directory missing: read={_readDir.FullName} post={_postDir.FullName}");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy(ex.Message);
        }
    }

    private void OnWatcherError(object sender, ErrorEventArgs e)
    {
        _logger.LogError(e.GetException(), "FileSystemWatcher error on '{Dir}' — some file events may have been lost", _readDir.FullName);
        // Re-scan so any files whose events were dropped are still queued. This runs on a thread pool callback, where an
        // exception (the folder itself may be gone, which is why the watcher failed) would end the process.
        try { QueueExistingFiles(); }
        catch (Exception ex) { _logger.LogError(ex, "Could not re-scan '{Dir}' after the watcher error", _readDir.FullName); }
    }

    private void OnFileCreated(object sender, FileSystemEventArgs e)
    {
        _fileChannel?.Writer.TryWrite(new FileInfo(e.FullPath));
    }

    private async Task ProcessChannelAsync(ChannelReader<FileInfo> reader, Func<byte[], CancellationToken, Task<ProcessResult>> handler,
        CancellationToken stopping, CancellationToken handlerToken)
    {
        try
        {
            await foreach (var file in reader.ReadAllAsync(stopping))
            {
                if (!IsDue(file))
                {
                    WakeWhenDue(file, stopping);
                    continue;
                }
                var content = await ReadAndArchiveAsync(file, stopping);
                if (content == null) continue;

                var result = await EndpointHandler.InvokeAsync(handler, Encoding.UTF8.GetBytes(content), _logger, _name, handlerToken, file.Name);
                // The file was already renamed to .processed, so it cannot be redelivered; the broker never
                // asks this endpoint to dead-letter (UsesNativeDeadLetter is false).
                if (result.Outcome != ProcessOutcome.Completed)
                    _logger.LogError(result.Exception, "File '{File}' was not processed ({Outcome}: {Failure}) and will not be retried",
                        file.Name, result.Outcome, result.FailureText);
            }
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested || handlerToken.IsCancellationRequested) { }
        catch (Exception ex) { _logger.LogCritical(ex, "File processing loop terminated unexpectedly for '{Dir}'", _readDir.FullName); }
    }

    private async Task<string?> ReadAndArchiveAsync(FileInfo file, CancellationToken ct)
    {
        try
        {
            return await _fileReadyPolicy.ExecuteAsync(async token =>
            {
                using var fs = new FileStream(file.FullName, FileMode.Open, FileAccess.Read,
                    FileShare.ReadWrite | FileShare.Delete, bufferSize: 4096, useAsync: false);
                using var reader = new StreamReader(fs);
                var content = await reader.ReadToEndAsync(token);
                // Guard: writer may not have flushed yet when the Created event fires early.
                // Throw so the retry policy retries; the file is NOT renamed and stays available for re-read.
                if (content.Length == 0)
                    throw new IOException("File has no content yet.");
                // Rename to .processed so it is not picked up again.
                var processed = Path.ChangeExtension(file.FullName, ".processed");
                System.IO.File.Move(file.FullName, processed, overwrite: true);
                return content;
            }, ct);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            _logger.LogWarning("Could not read '{File}' after retries: {Error}", file.Name, ex.Message);
            return null;
        }
    }
}
