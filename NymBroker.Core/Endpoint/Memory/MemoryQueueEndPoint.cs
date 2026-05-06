using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using NymBroker.Core.Endpoint.HealthCheck;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace NymBroker.Core.Endpoint.Memory;

/// <summary>In-process endpoint backed by a bounded Channel. Useful for testing and inter-component routing.</summary>
public sealed class MemoryQueueEndPoint : IEndPointEventDriven, IEndPointPoll
{
    private readonly Channel<string> _channel;
    private readonly ILogger<MemoryQueueEndPoint> _logger;

    public string Name { get; }

    public MemoryQueueEndPoint(string name, int capacity = 1000, ILogger<MemoryQueueEndPoint>? logger = null)
    {
        Name = name;
        _logger = logger ?? NullLogger<MemoryQueueEndPoint>.Instance;
        _channel = Channel.CreateBounded<string>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false
        });
    }

    public async Task PostAsync(Stream message, CancellationToken ct = default)
    {
        using var reader = new StreamReader(message, Encoding.UTF8, leaveOpen: true);
        var json = await reader.ReadToEndAsync(ct);
        await _channel.Writer.WriteAsync(json, ct);
    }

    public Task StartListeningAsync(Func<string, CancellationToken, Task> handler, CancellationToken ct)
    {
        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var msg in _channel.Reader.ReadAllAsync(ct))
                {
                    try
                    {
                        await handler(msg, ct);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger.LogError(ex, "Unhandled error dispatching message on endpoint '{Name}'", Name);
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Listener loop for endpoint '{Name}' terminated unexpectedly", Name);
            }
        }, ct);
        return Task.CompletedTask;
    }

    public Task StopListeningAsync()
    {
        _channel.Writer.TryComplete();
        return Task.CompletedTask;
    }

    public async IAsyncEnumerable<string> ReadAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        while (_channel.Reader.TryRead(out var msg))
        {
            if (ct.IsCancellationRequested) yield break;
            yield return msg;
            await Task.Yield();
        }
    }

    public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();

    /// <summary>Direct enqueue without a Stream — convenient for tests.</summary>
    public ValueTask EnqueueAsync(string json, CancellationToken ct = default)
        => _channel.Writer.WriteAsync(json, ct);
}
