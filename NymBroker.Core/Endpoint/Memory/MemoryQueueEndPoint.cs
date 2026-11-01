using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using NymBroker.Core.Endpoint.HealthCheck;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace NymBroker.Core.Endpoint.Memory;

/// <summary>In-process endpoint backed by a bounded Channel. Useful for testing and inter-component routing.</summary>
public sealed class MemoryQueueEndPoint : IEndPointEventDriven, IDeferredEndPoint
{
    // Task.Delay rejects longer waits (about 49 days), so a longer deferral waits in steps.
    private static readonly TimeSpan MaxDelayStep = TimeSpan.FromDays(1);

    private readonly CancellationTokenSource _stopped = new();
    private readonly Channel<byte[]> _channel;
    private readonly ILogger<MemoryQueueEndPoint> _logger;
    private readonly string _name;

    public EndpointMode Mode { get; }

    public MemoryQueueEndPoint(string name, int capacity = 1000, ILogger<MemoryQueueEndPoint>? logger = null, EndpointMode mode = EndpointMode.ReadWrite)
    {
        _name = name;
        Mode = mode;
        _logger = logger ?? NullLogger<MemoryQueueEndPoint>.Instance;
        _channel = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });
    }

    public Task PostAsync(byte[] message, CancellationToken ct = default)
        => _channel.Writer.WriteAsync(message, ct).AsTask();

    /// <summary>
    /// Holds the message in memory until it is due, then queues it. It does not count towards the capacity while it
    /// waits, and like every message on this endpoint it does not survive a restart: one still waiting when the endpoint
    /// stops is dropped with a warning.
    /// </summary>
    public Task PostDeferredAsync(byte[] message, DateTimeOffset deliverAt, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        if (deliverAt <= DateTimeOffset.UtcNow) return PostAsync(message, ct);
        if (_stopped.IsCancellationRequested)
            throw new ChannelClosedException($"Memory endpoint '{_name}' has stopped.");

        _ = DeliverWhenDueAsync(message, deliverAt);
        return Task.CompletedTask;
    }

    private async Task DeliverWhenDueAsync(byte[] message, DateTimeOffset deliverAt)
    {
        try
        {
            for (var wait = deliverAt - DateTimeOffset.UtcNow; wait > TimeSpan.Zero; wait = deliverAt - DateTimeOffset.UtcNow)
                await Task.Delay(wait < MaxDelayStep ? wait : MaxDelayStep, _stopped.Token);

            await _channel.Writer.WriteAsync(message, _stopped.Token);
        }
        catch (Exception ex) when (ex is OperationCanceledException or ChannelClosedException)
        {
            _logger.LogWarning("A deferred message for {DeliverAt} on endpoint '{Name}' was dropped because the endpoint stopped", deliverAt, _name);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "A deferred message for {DeliverAt} on endpoint '{Name}' was lost", deliverAt, _name);
        }
    }

    public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct)
    {
        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var msg in _channel.Reader.ReadAllAsync(ct))
                {
                    var result = await EndpointHandler.InvokeAsync(handler, msg, _logger, _name, ct);
                    // In-memory messages cannot be redelivered, and the broker never asks this endpoint to
                    // dead-letter (UsesNativeDeadLetter is false), so anything but Completed means the message is lost.
                    if (result.Outcome != ProcessOutcome.Completed)
                        _logger.LogError(result.Exception, "Message on endpoint '{Name}' was not processed ({Outcome}: {Failure}) and cannot be redelivered",
                            _name, result.Outcome, result.FailureText);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Listener loop for endpoint '{_name}' terminated unexpectedly", _name);
            }
        }, ct);
        return Task.CompletedTask;
    }

    public Task StopListeningAsync()
    {
        _stopped.Cancel();
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
