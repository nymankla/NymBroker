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
    // A handler that never returns must not keep the broker from stopping.
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(30);
    // A channel write cancelled by the stop ends at once; this only bounds a surprise.
    private static readonly TimeSpan DeferredStopTimeout = TimeSpan.FromSeconds(10);

    private readonly record struct DeferredMessage(byte[] Message, DateTimeOffset DeliverAt);

    private readonly int _capacity;
    private readonly ILogger<MemoryQueueEndPoint> _logger;
    private readonly string _name;
    // One loop for every waiting message (#117).
    private readonly DeferredQueue<DeferredMessage> _deferred;
    private volatile bool _stopped;
    private Channel<byte[]> _channel;
    private CancellationTokenSource? _listeningCts;
    private Task? _loop;

    public EndpointMode Mode { get; }

    public MemoryQueueEndPoint(string name, int capacity = 1000, ILogger<MemoryQueueEndPoint>? logger = null, EndpointMode mode = EndpointMode.ReadWrite)
    {
        _name = name;
        Mode = mode;
        _capacity = capacity;
        _logger = logger ?? NullLogger<MemoryQueueEndPoint>.Instance;
        _channel = NewChannel();
        _deferred = new DeferredQueue<DeferredMessage>(DeliverDueAsync, LogDropped, _logger, name);
    }

    private Channel<byte[]> NewChannel() => Channel.CreateBounded<byte[]>(new BoundedChannelOptions(_capacity)
    {
        FullMode = BoundedChannelFullMode.Wait,
        SingleReader = true,
        SingleWriter = false
    });

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
        if (!_deferred.TryAdd(new DeferredMessage(message, deliverAt), deliverAt))
            throw new ChannelClosedException($"Memory endpoint '{_name}' has stopped.");

        return Task.CompletedTask;
    }

    private async Task DeliverDueAsync(DeferredMessage deferred, CancellationToken stopped)
    {
        try
        {
            await _channel.Writer.WriteAsync(deferred.Message, stopped);
        }
        catch (Exception ex) when (ex is OperationCanceledException or ChannelClosedException)
        {
            LogDropped(deferred);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "A deferred message for {DeliverAt} on endpoint '{Name}' was lost", deferred.DeliverAt, _name);
        }
    }

    private void LogDropped(DeferredMessage deferred)
        => _logger.LogWarning("A deferred message for {DeliverAt} on endpoint '{Name}' was dropped because the endpoint stopped", deferred.DeliverAt, _name);

    /// <summary>Starts reading on the thread pool and returns. After a stop, listening again takes up where it left off.</summary>
    /// <exception cref="InvalidOperationException">The endpoint is already listening.</exception>
    public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct)
    {
        if (_loop is not null)
            throw new InvalidOperationException($"Memory endpoint '{_name}' is already listening.");

        if (_stopped)
            Reopen();

        _listeningCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = _listeningCts.Token;
        var reader = _channel.Reader;
        _loop = Task.Run(() => RunListenerAsync(handler, reader, token), CancellationToken.None);
        return Task.CompletedTask;
    }

    // Messages left over from a stop without a listener move along; none are lost.
    private void Reopen()
    {
        var old = _channel;
        var channel = NewChannel();
        while (old.Reader.TryRead(out var message))
            channel.Writer.TryWrite(message);

        _channel = channel;
        _stopped = false;
        _deferred.Restart();
    }

    private async Task RunListenerAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, ChannelReader<byte[]> reader, CancellationToken token)
    {
        try
        {
            await foreach (var msg in reader.ReadAllAsync(token))
            {
                var result = await EndpointHandler.InvokeAsync(handler, msg, _logger, _name, token);
                // In-memory messages cannot be redelivered, and the broker never asks this endpoint to
                // dead-letter (UsesNativeDeadLetter is false), so anything but Completed means the message is lost.
                if (result.Outcome != ProcessOutcome.Completed)
                    _logger.LogError(result.Exception, "Message on endpoint '{Name}' was not processed ({Outcome}: {Failure}) and cannot be redelivered",
                        _name, result.Outcome, result.FailureText);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Listener loop for endpoint '{Name}' terminated unexpectedly", _name);
        }
    }

    /// <summary>
    /// Refuses new messages, drops those still waiting for their time, then handles what is already queued and returns, so
    /// no handler runs after this. After 30 seconds the running handler is cancelled and what is left stays queued.
    /// </summary>
    public async Task StopListeningAsync()
    {
        _stopped = true;
        if (!await _deferred.StopAsync(DeferredStopTimeout))
            _logger.LogWarning("Memory endpoint '{Name}' was still queueing a deferred message {Timeout} after it began to stop", _name, DeferredStopTimeout);
        _channel.Writer.TryComplete();

        if (_loop is null) return;

        try
        {
            await _loop.WaitAsync(DrainTimeout);
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("Memory endpoint '{Name}' did not finish its queue within {Timeout}; cancelling the running handler", _name, DrainTimeout);
            await _listeningCts!.CancelAsync();
            await _loop;
        }

        _loop = null;
        _listeningCts!.Dispose();
        _listeningCts = null;
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
