using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using MessageBroker.Core.Endpoint.HealthCheck;

namespace MessageBroker.Core.Endpoint.Memory;

/// <summary>In-process endpoint backed by a bounded Channel. Useful for testing and inter-component routing.</summary>
public sealed class MemoryQueueEndPoint : IEndPointEventDriven, IEndPointPoll
{
    private readonly Channel<string> _channel;

    public string Name { get; }

    public MemoryQueueEndPoint(string name, int capacity = 1000)
    {
        Name = name;
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
            await foreach (var msg in _channel.Reader.ReadAllAsync(ct))
                await handler(msg, ct);
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
