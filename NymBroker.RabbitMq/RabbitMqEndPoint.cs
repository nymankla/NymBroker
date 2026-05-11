using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace NymBroker.RabbitMq;

public sealed class RabbitMqEndPoint : IEndPointEventDriven, IAsyncDisposable
{
    private readonly RabbitMqSettings _settings;
    private readonly ILogger<RabbitMqEndPoint> _logger;
    private readonly ResiliencePipeline _reconnectPolicy;
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

        _reconnectPolicy = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = int.MaxValue,
                Delay = TimeSpan.FromSeconds(settings.ReconnectDelaySeconds),
                OnRetry = args =>
                {
                    _logger.LogWarning("RabbitMQ [{Name}] reconnecting (attempt {Attempt}): {Error}",
                        _name, args.AttemptNumber + 1, args.Outcome.Exception?.Message);
                    return ValueTask.CompletedTask;
                }
            })
            .Build();
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

    public async Task StartListeningAsync(Func<byte[], CancellationToken, Task> handler, CancellationToken ct)
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

                    var consumer = new AsyncEventingBasicConsumer(channel);
                    consumer.ReceivedAsync += async (_, ea) =>
                    {
                        try
                        {
                            await handler(ea.Body.ToArray(), token);
                            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, cancellationToken: token);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error processing message from {Queue}", _settings.ReadQueueName);
                            await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true, cancellationToken: token);
                        }
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
