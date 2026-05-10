using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using NymBroker.Core.Endpoint.HealthCheck;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace NymBroker.Core.Endpoint.Memory;

/// <summary>In-process endpoint backed by a bounded Channel. Useful for testing and inter-component routing.</summary>
public sealed class MemoryQueueEndPoint : IEndPointEventDriven
{
    private readonly Channel<byte[]> _channel;
    private readonly ILogger<MemoryQueueEndPoint> _logger;

    public string Name { get; }

    public MemoryQueueEndPoint(string name, int capacity = 1000, ILogger<MemoryQueueEndPoint>? logger = null)
    {
        Name = name;
        _logger = logger ?? NullLogger<MemoryQueueEndPoint>.Instance;
        _channel = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });
    }

    public async Task PostAsync(Stream message, CancellationToken ct = default)
    {
        byte[] bytes;
        if (message is MemoryStream ms && ms.TryGetBuffer(out var buf))
        {
            bytes = new byte[buf.Count];
            buf.Array!.AsSpan(buf.Offset, buf.Count).CopyTo(bytes);
        }
        else
        {
            using var ms2 = new MemoryStream();
            await message.CopyToAsync(ms2, ct);
            bytes = ms2.ToArray();
        }
        await _channel.Writer.WriteAsync(bytes, ct);
    }

    public Task StartListeningAsync(Func<byte[], CancellationToken, Task> handler, CancellationToken ct)
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
        while (_channel.Reader.TryRead(out var bytes))
        {
            if (ct.IsCancellationRequested) yield break;
            yield return Encoding.UTF8.GetString(bytes);
            await Task.Yield();
        }
    }

    public IHealthCheckResult HealthCheck() => HealthCheckResult.Healthy();

    /// <summary>Direct enqueue without a Stream — convenient for tests.</summary>
    public ValueTask EnqueueAsync(string json, CancellationToken ct = default)
        => _channel.Writer.WriteAsync(Encoding.UTF8.GetBytes(json), ct);
}
