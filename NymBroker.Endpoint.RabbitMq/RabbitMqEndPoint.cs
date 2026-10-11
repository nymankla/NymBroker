using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Resilience;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace NymBroker.Endpoint.RabbitMq;

public sealed class RabbitMqEndPoint : IEndPointEventDriven, IAsyncDisposable
{
    private readonly RabbitMqSettings _settings;
    private readonly ILogger<RabbitMqEndPoint> _logger;
    private readonly RetryPolicy _reconnectPolicy;
    private readonly string _name;

    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly SemaphoreSlim _publishChannelLock = new(1, 1);
    // Held while a delivery is handled and settled, so StopListeningAsync can wait for it and flush the ack batch.
    private readonly SemaphoreSlim _deliveryLock = new(1, 1);

    private IConnection? _connection;
    private IChannel? _publishChannel;
    private IChannel? _consumeChannel;

    // Listener state; the batch fields are only touched under _deliveryLock.
    private CancellationTokenSource? _listeningCts;
    private Task? _loop;
    private string? _consumerTag;
    private volatile bool _stopping;
    private int _pendingAcks;
    private ulong _lastGoodTag;

    public EndpointMode Mode { get; }

    public RabbitMqEndPoint(string name, RabbitMqSettings settings, ILogger<RabbitMqEndPoint> logger, EndpointMode mode = EndpointMode.ReadWrite)
    {
        _name = name;
        Mode = mode;
        _settings = settings;
        _logger = logger;

        _reconnectPolicy = new RetryPolicy(new RetryOptions
        {
            MaxRetryAttempts = int.MaxValue,
            Delay = TimeSpan.FromSeconds(settings.ReconnectDelaySeconds),
            OnRetry = args =>
            {
                _logger.LogWarning("RabbitMQ [{Name}] reconnecting (attempt {Attempt}): {Error}",
                    _name, args.AttemptNumber + 1, args.Exception.Message);
                return ValueTask.CompletedTask;
            }
        });
    }

    public async Task PostAsync(byte[] message, CancellationToken ct = default)
    {
        var channel = await EnsurePublishChannelAsync(ct);
        await channel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: _settings.WriteQueueName,
            body: message,
            cancellationToken: ct);
    }

    /// <summary>Dead-letters via <c>BasicNack(requeue: false)</c>, i.e. the queue's dead-letter exchange.</summary>
    public bool UsesNativeDeadLetter => _settings.UseNativeDeadLetter;

    public Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(_settings.ReadQueueName))
            throw new InvalidOperationException($"RabbitMQ endpoint '{_name}' has no ReadQueueName configured.");
        if (_loop is not null)
            throw new InvalidOperationException($"RabbitMQ endpoint '{_name}' is already listening.");

        // Our own token: the one passed in is the broker's startup token, which stopping does not cancel.
        _listeningCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = _listeningCts.Token;
        _stopping = false;
        _loop = Task.Run(() => RunListenerAsync(handler, token), CancellationToken.None);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Stops deliveries, waits for the message being handled, acks the pending batch, then closes the consume channel and
    /// waits for the listener loop — so no handler runs after this returns and no handled message is redelivered.
    /// Prefetched messages that were not handled are requeued by RabbitMQ when the channel closes.
    /// </summary>
    public async Task StopListeningAsync()
    {
        if (_loop is null) return;
        _stopping = true;

        var channel = _consumeChannel;
        if (channel?.IsOpen == true && _consumerTag is not null)
        {
            try { await channel.BasicCancelAsync(_consumerTag); }
            catch (Exception ex) { _logger.LogWarning(ex, "RabbitMQ [{Name}] could not cancel the consumer cleanly", _name); }
        }

        _listeningCts?.Cancel();   // the running handler sees the stop; the reconnect loop ends

        await _deliveryLock.WaitAsync();
        try
        {
            if (channel is not null) await FlushAcksAsync(channel);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "RabbitMQ [{Name}] could not ack {Count} handled message(s) on stop; they will be redelivered", _name, _pendingAcks);
        }
        finally
        {
            _deliveryLock.Release();
        }

        await _loop;
        _loop = null;
        _listeningCts?.Dispose();
        _listeningCts = null;

        if (_consumeChannel is not null)
        {
            try { await _consumeChannel.CloseAsync(); }
            catch (Exception ex) { _logger.LogWarning(ex, "RabbitMQ [{Name}] could not close the consume channel cleanly", _name); }
            await _consumeChannel.DisposeAsync();
            _consumeChannel = null;
        }
    }

    private async Task RunListenerAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken token)
    {
        try
        {
            await _reconnectPolicy.ExecuteAsync(async t =>
            {
                var channel = await EnsureConsumeChannelAsync(t);
                _pendingAcks = 0;   // a reconnect starts a new channel; tags of the old one are void (RabbitMQ requeued them)
                await channel.QueueDeclareAsync(_settings.ReadQueueName, durable: true,
                    exclusive: false, autoDelete: false, cancellationToken: t);

                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += (_, ea) => OnDeliveryAsync(channel, ea, handler, token);

                _consumerTag = await channel.BasicConsumeAsync(_settings.ReadQueueName, autoAck: false, consumer: consumer, cancellationToken: t);

                await Task.Delay(Timeout.Infinite, t);
            }, token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "RabbitMQ [{Name}] listener loop terminated unexpectedly", _name);
        }
    }

    private async Task OnDeliveryAsync(IChannel channel, BasicDeliverEventArgs ea, Func<byte[], CancellationToken, Task<ProcessResult>> handler,
        CancellationToken token)
    {
        await _deliveryLock.WaitAsync(CancellationToken.None);
        try
        {
            // Already dispatched before the consumer was cancelled: leave it unacked, RabbitMQ requeues it on close.
            if (_stopping) return;

            ProcessResult result;
            try
            {
                result = await EndpointHandler.InvokeAsync(handler, ea.Body.ToArray(), _logger, _name, token, ea.DeliveryTag);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return;   // the handler stopped because we are stopping: unsettled, requeued when the channel closes
            }

            // Settled with CancellationToken.None: a handled message must be acked even while stopping, or it is redelivered.
            await SettleAsync(channel, ea, result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "RabbitMQ [{Name}] could not settle message {DeliveryTag}; it will be redelivered", _name, ea.DeliveryTag);
        }
        finally
        {
            _deliveryLock.Release();
        }
    }

    // Must be called while holding _deliveryLock.
    private async Task SettleAsync(IChannel channel, BasicDeliverEventArgs ea, ProcessResult result)
    {
        if (result.Outcome == ProcessOutcome.Completed)
        {
            _lastGoodTag = ea.DeliveryTag;
            if (++_pendingAcks >= _settings.BatchAckSize)
                await FlushAcksAsync(channel);
            return;
        }

        // Ack the successful messages before this one, then settle this one on its own.
        await FlushAcksAsync(channel);

        if (result.Outcome == ProcessOutcome.DeadLetter)
        {
            // Reject without requeue: RabbitMQ routes it to the queue's dead-letter exchange, if one is configured.
            _logger.LogWarning("Message {DeliveryTag} from {Queue} rejected without requeue (dead-lettered if the queue has a DLX): {Failure}",
                ea.DeliveryTag, _settings.ReadQueueName, result.FailureText);
            await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false, cancellationToken: CancellationToken.None);
            return;
        }

        // Retry. A message that already failed once after redelivery is treated as poison: reject
        // without requeue so it dead-letters (if a DLX is configured) instead of looping forever.
        var requeue = !(_settings.RejectRedeliveredFailures && ea.Redelivered);
        if (requeue)
            _logger.LogWarning("Message {DeliveryTag} from {Queue} will be redelivered: {Failure}", ea.DeliveryTag, _settings.ReadQueueName, result.FailureText);
        else
            _logger.LogWarning("Message {DeliveryTag} from {Queue} failed again after redelivery; rejecting without requeue (poison message): {Failure}",
                ea.DeliveryTag, _settings.ReadQueueName, result.FailureText);
        await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: requeue, cancellationToken: CancellationToken.None);
    }

    // Must be called while holding _deliveryLock.
    private async Task FlushAcksAsync(IChannel channel)
    {
        if (_pendingAcks == 0) return;
        await channel.BasicAckAsync(_lastGoodTag, multiple: true, cancellationToken: CancellationToken.None);
        _pendingAcks = 0;
    }

    public IHealthCheckResult HealthCheck()
    {
        return _connection?.IsOpen == true
            ? HealthCheckResult.Healthy()
            : HealthCheckResult.Unhealthy($"RabbitMQ [{_name}] not connected");
    }

    public async ValueTask DisposeAsync()
    {
        await StopListeningAsync();
        if (_publishChannel != null) await _publishChannel.DisposeAsync();
        if (_connection != null) await _connection.DisposeAsync();
        _publishChannelLock.Dispose();
        _connectionLock.Dispose();
        _deliveryLock.Dispose();
    }

    private async Task<IChannel> EnsurePublishChannelAsync(CancellationToken ct)
    {
        if (_publishChannel?.IsOpen == true) return _publishChannel;

        await _publishChannelLock.WaitAsync(ct);
        try
        {
            if (_publishChannel?.IsOpen == true) return _publishChannel;

            var conn = await EnsureConnectionAsync(ct);
            _publishChannel = await conn.CreateChannelAsync(cancellationToken: ct);
            await _publishChannel.QueueDeclareAsync(_settings.WriteQueueName, durable: true,
                exclusive: false, autoDelete: false, cancellationToken: ct);
            return _publishChannel;
        }
        finally
        {
            _publishChannelLock.Release();
        }
    }

    private async Task<IChannel> EnsureConsumeChannelAsync(CancellationToken ct)
    {
        if (_consumeChannel?.IsOpen == true) return _consumeChannel;
        var conn = await EnsureConnectionAsync(ct);
        _consumeChannel = await conn.CreateChannelAsync(cancellationToken: ct);
        // Backstop: RabbitMQ.Client reports exceptions escaping a consumer callback here, not to the caller.
        _consumeChannel.CallbackExceptionAsync += (_, args) =>
        {
            _logger.LogError(args.Exception, "RabbitMQ [{Name}] consumer callback failed", _name);
            return Task.CompletedTask;
        };
        return _consumeChannel;
    }

    internal static ConnectionFactory CreateConnectionFactory(RabbitMqSettings settings) => new()
    {
        HostName = settings.HostName,
        Port = settings.Port,
        UserName = settings.User,
        Password = settings.Password,
        VirtualHost = settings.VirtualHost,
        // AcceptablePolicyErrors stays None: an untrusted, expired or mismatched server certificate fails the connection.
        Ssl = new SslOption
        {
            Enabled = settings.UseTls,
            ServerName = settings.TlsServerName ?? settings.HostName,
            CertPath = settings.ClientCertificatePath ?? string.Empty,
            CertPassphrase = settings.ClientCertificatePassword ?? string.Empty
        }
    };

    private async Task<IConnection> EnsureConnectionAsync(CancellationToken ct)
    {
        if (_connection?.IsOpen == true) return _connection;

        await _connectionLock.WaitAsync(ct);
        try
        {
            if (_connection?.IsOpen == true) return _connection;

            _connection = await CreateConnectionFactory(_settings).CreateConnectionAsync(ct);
            _logger.LogInformation("RabbitMQ [{Name}] connected to {Host}:{Port}{Tls}", _name, _settings.HostName, _settings.Port,
                _settings.UseTls ? " (TLS)" : "");
            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }
}
