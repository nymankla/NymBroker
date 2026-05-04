using MessageBroker.Core.Endpoint.HealthCheck;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Core.Endpoint.File;

public sealed class FileEndPoint : IEndPointEventDriven, IEndPointPoll
{
    private readonly FileSettings _settings;
    private readonly ILogger<FileEndPoint> _logger;
    private readonly DirectoryInfo _readDir;
    private readonly DirectoryInfo _postDir;
    private FileSystemWatcher? _watcher;
    private Func<string, CancellationToken, Task>? _handler;
    private CancellationToken _listenerCt;

    public string Name { get; }

    public FileEndPoint(string name, FileSettings settings, ILogger<FileEndPoint> logger)
    {
        Name = name;
        _settings = settings;
        _logger = logger;

        var basePath = settings.IsAbsolutePath
            ? string.Empty
            : AppContext.BaseDirectory;

        _readDir = new DirectoryInfo(Path.Combine(basePath, settings.ReadPath));
        _postDir = new DirectoryInfo(Path.Combine(basePath, settings.PostPath));

        _readDir.Create();
        _postDir.Create();
    }

    public async Task PostAsync(Stream message, CancellationToken ct = default)
    {
        var fileName = $"{DateTime.UtcNow:yyyyMMddHHmmss}_{Guid.NewGuid():N}.json";
        var path = Path.Combine(_postDir.FullName, fileName);

        await using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 65536, useAsync: true);
        await message.CopyToAsync(fs, ct);
    }

    public Task StartListeningAsync(Func<string, CancellationToken, Task> handler, CancellationToken ct)
    {
        _handler = handler;
        _listenerCt = ct;

        _watcher = new FileSystemWatcher(_readDir.FullName, _settings.SearchPattern)
        {
            NotifyFilter = NotifyFilters.FileName,
            EnableRaisingEvents = true
        };
        _watcher.Created += OnFileCreated;

        // Also process any files already present.
        _ = Task.Run(() => ProcessExistingFilesAsync(ct), ct);

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

    private void OnFileCreated(object sender, FileSystemEventArgs e)
    {
        if (_handler == null) return;
        _ = Task.Run(async () =>
        {
            // Brief delay so the writer can finish flushing.
            await Task.Delay(50, _listenerCt);
            var file = new FileInfo(e.FullPath);
            var content = await ReadAndArchiveAsync(file, _listenerCt);
            if (content != null)
                await _handler(content, _listenerCt);
        }, _listenerCt);
    }

    private async Task ProcessExistingFilesAsync(CancellationToken ct)
    {
        foreach (var file in _readDir.GetFiles(_settings.SearchPattern))
        {
            if (ct.IsCancellationRequested) return;
            var content = await ReadAndArchiveAsync(file, ct);
            if (content != null && _handler != null)
                await _handler(content, ct);
        }
    }

    private async Task<string?> ReadAndArchiveAsync(FileInfo file, CancellationToken ct)
    {
        try
        {
            await using var fs = new FileStream(file.FullName, FileMode.Open, FileAccess.Read,
                FileShare.None, bufferSize: 65536, useAsync: true);
            using var reader = new StreamReader(fs);
            var content = await reader.ReadToEndAsync(ct);

            // Rename to .processed so it is not picked up again.
            var processed = Path.ChangeExtension(file.FullName, ".processed");
            System.IO.File.Move(file.FullName, processed, overwrite: true);

            return content;
        }
        catch (IOException ex)
        {
            // File may still be locked by the writer — will be retried on next poll.
            _logger.LogDebug("Could not read {File}: {Error}", file.Name, ex.Message);
            return null;
        }
    }
}
