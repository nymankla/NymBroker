using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Resilience;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace NymBroker.RabbitMq;

public sealed class RabbitMqEndPoint : IEndPointEventDriven, IAsyncDisposable
{
    private readonly RabbitMqSettings _settings;
    private readonly ILogger<RabbitMqEndPoint> _logger;
    private readonly RetryPolicy _reconnectPolicy;
    private readonly string _name;

    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly SemaphoreSlim _publishChannelLock = new(1, 1);

    private IConnection? _connection;
    private IChannel? _publishChannel;
    private IChannel? _consumeChannel;

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

    public async Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(_settings.ReadQueueName))
            throw new InvalidOperationException($"RabbitMQ endpoint '{_name}' has no ReadQueueName configured.");

        _ = Task.Run(async () =>
        {
            try
            {
                await _reconnectPolicy.ExecuteAsync(async token =>
                {
                    var channel = await EnsureConsumeChannelAsync(token);
                    await channel.QueueDeclareAsync(_settings.ReadQueueName, durable: true,
                        exclusive: false, autoDelete: false, cancellationToken: token);

                    var batchSize    = _settings.BatchAckSize;
                    var pendingCount = 0;
                    var lastGoodTag  = 0UL;

                    var consumer = new AsyncEventingBasicConsumer(channel);
                    consumer.ReceivedAsync += async (_, ea) =>
                    {
                        ProcessResult result;
                        try
                        {
                            result = await handler(ea.Body.ToArray(), token);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error processing message from {Queue}", _settings.ReadQueueName);
                            result = ProcessResult.Retry(ex);
                        }

                        if (result.Outcome == ProcessOutcome.Completed)
                        {
                            lastGoodTag = ea.DeliveryTag;
                            if (++pendingCount >= batchSize)
                            {
                                await channel.BasicAckAsync(lastGoodTag, multiple: true, cancellationToken: token);
                                pendingCount = 0;
                            }
                            return;
                        }

                        // Ack the successful messages before this one, then settle this one on its own.
                        if (pendingCount > 0)
                        {
                            await channel.BasicAckAsync(lastGoodTag, multiple: true, cancellationToken: token);
                            pendingCount = 0;
                        }

                        if (result.Outcome == ProcessOutcome.DeadLetter)
                        {
                            // Reject without requeue: RabbitMQ routes it to the queue's dead-letter exchange, if one is configured.
                            _logger.LogWarning("Message {DeliveryTag} from {Queue} rejected without requeue (dead-lettered if the queue has a DLX): {Failure}",
                                ea.DeliveryTag, _settings.ReadQueueName, result.FailureText);
                            await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false, cancellationToken: token);
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
                        await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: requeue, cancellationToken: token);
                    };

                    await channel.BasicConsumeAsync(_settings.ReadQueueName, autoAck: false, consumer: consumer, cancellationToken: token);

                    await Task.Delay(Timeout.Infinite, token);
                }, ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogCritical(ex, "RabbitMQ [{Name}] listener loop terminated unexpectedly", _name);
            }
        }, ct);
    }

    public async Task StopListeningAsync()
    {
        if (_consumeChannel != null)
        {
            await _consumeChannel.CloseAsync();
            _consumeChannel = null;
        }
    }

    public IHealthCheckResult HealthCheck()
    {
        return _connection?.IsOpen == true
            ? HealthCheckResult.Healthy()
            : HealthCheckResult.Unhealthy($"RabbitMQ [{_name}] not connected");
    }

    public async ValueTask DisposeAsync()
    {
        if (_publishChannel != null) await _publishChannel.DisposeAsync();
        if (_consumeChannel != null) await _consumeChannel.DisposeAsync();
        if (_connection != null) await _connection.DisposeAsync();
        _publishChannelLock.Dispose();
        _connectionLock.Dispose();
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
        return _consumeChannel;
    }

    private async Task<IConnection> EnsureConnectionAsync(CancellationToken ct)
    {
        if (_connection?.IsOpen == true) return _connection;

        await _connectionLock.WaitAsync(ct);
        try
        {
            if (_connection?.IsOpen == true) return _connection;

            var factory = new ConnectionFactory
            {
                HostName = _settings.HostName,
                Port = _settings.Port,
                UserName = _settings.User,
                Password = _settings.Password,
                VirtualHost = _settings.VirtualHost
            };

            _connection = await factory.CreateConnectionAsync(ct);
            _logger.LogInformation("RabbitMQ [{Name}] connected to {Host}:{Port}", _name, _settings.HostName, _settings.Port);
            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }
}
