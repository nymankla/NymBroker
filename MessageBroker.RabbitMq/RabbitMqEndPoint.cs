using System.Text;
using MessageBroker.Core.Endpoint;
using MessageBroker.Core.Endpoint.HealthCheck;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MessageBroker.RabbitMq;

public sealed class RabbitMqEndPoint : IEndPointEventDriven, IAsyncDisposable
{
    private readonly RabbitMqSettings _settings;
    private readonly ILogger<RabbitMqEndPoint> _logger;
    private readonly ResiliencePipeline _reconnectPolicy;

    private IConnection? _connection;
    private IChannel? _publishChannel;
    private IChannel? _consumeChannel;

    public string Name { get; }

    public RabbitMqEndPoint(string name, RabbitMqSettings settings, ILogger<RabbitMqEndPoint> logger)
    {
        Name = name;
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
                        Name, args.AttemptNumber + 1, args.Outcome.Exception?.Message);
                    return ValueTask.CompletedTask;
                }
            })
            .Build();
    }

    public async Task PostAsync(Stream message, CancellationToken ct = default)
    {
        var channel = await EnsurePublishChannelAsync(ct);

        using var ms = new MemoryStream();
        await message.CopyToAsync(ms, ct);
        var body = ms.ToArray();

        await channel.BasicPublishAsync(
            exchange: string.Empty,
            routingKey: _settings.WriteQueueName,
            body: body,
            cancellationToken: ct);
    }

    public async Task StartListeningAsync(Func<string, CancellationToken, Task> handler, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(_settings.ReadQueueName))
            throw new InvalidOperationException($"RabbitMQ endpoint '{Name}' has no ReadQueueName configured.");

        _ = Task.Run(async () =>
        {
            await _reconnectPolicy.ExecuteAsync(async token =>
            {
                var channel = await EnsureConsumeChannelAsync(token);
                await channel.QueueDeclareAsync(_settings.ReadQueueName, durable: true,
                    exclusive: false, autoDelete: false, cancellationToken: token);

                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += async (_, ea) =>
                {
                    var json = Encoding.UTF8.GetString(ea.Body.Span);
                    try { await handler(json, token); }
                    catch (Exception ex) { _logger.LogError(ex, "Error processing message from {Queue}", _settings.ReadQueueName); }
                };

                await channel.BasicConsumeAsync(_settings.ReadQueueName, autoAck: true, consumer: consumer, cancellationToken: token);

                await Task.Delay(Timeout.Infinite, token);
            }, ct);
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
            : HealthCheckResult.Unhealthy($"RabbitMQ [{Name}] not connected");
    }

    public async ValueTask DisposeAsync()
    {
        if (_publishChannel != null) await _publishChannel.DisposeAsync();
        if (_consumeChannel != null) await _consumeChannel.DisposeAsync();
        if (_connection != null) await _connection.DisposeAsync();
    }

    private async Task<IChannel> EnsurePublishChannelAsync(CancellationToken ct)
    {
        if (_publishChannel?.IsOpen == true) return _publishChannel;

        var conn = await EnsureConnectionAsync(ct);
        _publishChannel = await conn.CreateChannelAsync(cancellationToken: ct);
        await _publishChannel.QueueDeclareAsync(_settings.WriteQueueName, durable: true,
            exclusive: false, autoDelete: false, cancellationToken: ct);
        return _publishChannel;
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

        var factory = new ConnectionFactory
        {
            HostName = _settings.HostName,
            Port = _settings.Port,
            UserName = _settings.User,
            Password = _settings.Password,
            VirtualHost = _settings.VirtualHost
        };

        _connection = await factory.CreateConnectionAsync(ct);
        _logger.LogInformation("RabbitMQ [{Name}] connected to {Host}:{Port}", Name, _settings.HostName, _settings.Port);
        return _connection;
    }
}
