using System.Text;
using NymBroker.Core.Endpoint.HealthCheck;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;

namespace NymBroker.Core.Endpoint.File;

public sealed class FileEndPoint : IEndPointEventDriven
{
    private readonly FileSettings _settings;
    private readonly ILogger<FileEndPoint> _logger;
    private readonly DirectoryInfo _readDir;
    private readonly DirectoryInfo _postDir;
    private readonly ResiliencePipeline<string?> _fileReadyPolicy;
    private FileSystemWatcher? _watcher;
    private Func<byte[], CancellationToken, Task>? _handler;
    private CancellationToken _listenerCt;

    public EndpointMode Mode { get; }

    public FileEndPoint(string name, FileSettings settings, ILogger<FileEndPoint> logger, EndpointMode mode = EndpointMode.ReadWrite)
    {
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

        _fileReadyPolicy = new ResiliencePipelineBuilder<string?>()
            .AddRetry(new RetryStrategyOptions<string?>
            {
                MaxRetryAttempts = 4,
                Delay = TimeSpan.FromMilliseconds(25),
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                ShouldHandle = static args => ValueTask.FromResult(args.Outcome.Exception is IOException),
                OnRetry = args =>
                {
                    _logger.LogDebug(
                        "Retrying file read after transient file access failure on attempt {Attempt}: {Error}",
                        args.AttemptNumber + 1,
                        args.Outcome.Exception?.Message);
                    return ValueTask.CompletedTask;
                }
            })
            .Build();
    }

    public Task PostAsync(byte[] message, CancellationToken ct = default)
    {
        var fileName = $"{DateTime.UtcNow:yyyyMMddHHmmss}_{Guid.NewGuid():N}.json";
        var path = Path.Combine(_postDir.FullName, fileName);
        return System.IO.File.WriteAllBytesAsync(path, message, ct);
    }

    public Task StartListeningAsync(Func<byte[], CancellationToken, Task> handler, CancellationToken ct)
    {
        _handler = handler;
        _listenerCt = ct;

        _watcher = new FileSystemWatcher(_readDir.FullName, _settings.SearchPattern)
        {
            NotifyFilter        = NotifyFilters.FileName,
            InternalBufferSize  = 65536,   // 64 KB — default 8 KB overflows under burst writes
            EnableRaisingEvents = true
        };
        _watcher.Created += OnFileCreated;
        _watcher.Error   += OnWatcherError;

        // Also process any files already present.
        _ = Task.Run(async () =>
        {
            try { await ProcessExistingFilesAsync(ct); }
            catch (OperationCanceledException) { }
            catch (Exception ex) { _logger.LogError(ex, "Error scanning existing files in '{Dir}'", _readDir.FullName); }
        }, ct);

        ct.Register(() => _watcher.EnableRaisingEvents = false);
        return Task.CompletedTask;
    }

    public Task StopListeningAsync()
    {
        if (_watcher != null)
        {
            _watcher.EnableRaisingEvents = false;
            _watcher.Dispose();
            _watcher = null;
        }
        return Task.CompletedTask;
    }

    public async IAsyncEnumerable<string> ReadAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var file in _readDir.GetFiles(_settings.SearchPattern))
        {
            if (ct.IsCancellationRequested) yield break;
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
        // Re-scan the directory so any files whose events were dropped are still processed.
        _ = Task.Run(async () =>
        {
            try { await ProcessExistingFilesAsync(_listenerCt); }
            catch (OperationCanceledException) { }
            catch (Exception ex) { _logger.LogError(ex, "Error re-scanning '{Dir}' after watcher overflow", _readDir.FullName); }
        }, _listenerCt);
    }

    private void OnFileCreated(object sender, FileSystemEventArgs e)
    {
        if (_handler == null) return;
        _ = Task.Run(async () =>
        {
            try
            {
                var file = new FileInfo(e.FullPath);
                var content = await ReadAndArchiveAsync(file, _listenerCt);
                if (content != null)
                    await _handler(Encoding.UTF8.GetBytes(content), _listenerCt);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unhandled error processing file '{File}'", e.Name);
            }
        }, _listenerCt);
    }

    private async Task ProcessExistingFilesAsync(CancellationToken ct)
    {
        foreach (var file in _readDir.GetFiles(_settings.SearchPattern))
        {
            if (ct.IsCancellationRequested) return;
            var content = await ReadAndArchiveAsync(file, ct);
            if (content != null && _handler != null)
            {
                try { await _handler(Encoding.UTF8.GetBytes(content), ct); }
                catch (Exception ex) when (ex is not OperationCanceledException)
                { _logger.LogError(ex, "Unhandled error processing existing file '{File}'", file.Name); }
            }
        }
    }

    private async Task<string?> ReadAndArchiveAsync(FileInfo file, CancellationToken ct)
    {
        try
        {
            return await _fileReadyPolicy.ExecuteAsync(async token =>
            {
                using var fs = new FileStream(file.FullName, FileMode.Open, FileAccess.Read,
                    FileShare.ReadWrite | FileShare.Delete, bufferSize: 65536, useAsync: false);
                using var reader = new StreamReader(fs);
                var content = await reader.ReadToEndAsync(token);
                // Rename to .processed so it is not picked up again.
                var processed = Path.ChangeExtension(file.FullName, ".processed");
                System.IO.File.Move(file.FullName, processed, overwrite: true);
                return content;
            }, ct);
        }
        catch (IOException ex)
        {
            _logger.LogWarning("Could not read '{File}' after retries: {Error}", file.Name, ex.Message);
            return null;
        }
    }
}
