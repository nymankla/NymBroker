using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;

namespace NymBroker.AzureServiceBus;

/// <summary>
/// Azure Service Bus queue or topic/subscription endpoint. Push-based: a <see cref="ServiceBusProcessor"/> (peek-lock,
/// manual settlement) calls the broker for each message and settles it by the returned <see cref="ProcessResult"/>:
/// Completed → complete, Retry → abandon (redelivered; the entity's MaxDeliveryCount dead-letters it), DeadLetter →
/// the entity's dead-letter queue with the reason. The client, sender and processor are long-lived and thread-safe.
/// Transient-fault retries are left to the SDK's built-in <see cref="ServiceBusRetryOptions"/>.
/// </summary>
public sealed class AzureServiceBusEndPoint : IEndPointEventDriven, IAsyncDisposable
{
    private static readonly TimeSpan HealthCheckTimeout = TimeSpan.FromSeconds(5);

    private readonly string _name;
    private readonly AzureServiceBusSettings _settings;
    private readonly ILogger<AzureServiceBusEndPoint> _logger;
    private readonly Lazy<ServiceBusClient> _client;
    private readonly Lazy<ServiceBusSender> _sender;
    private readonly SemaphoreSlim _lifecycleLock = new(1, 1);

    private ServiceBusProcessor? _processor;

    public AzureServiceBusEndPoint(string name, AzureServiceBusSettings settings, ILogger<AzureServiceBusEndPoint> logger,
        EndpointMode mode = EndpointMode.ReadWrite)
    {
        settings.Validate(name);
        _name = name;
        _settings = settings;
        _logger = logger;
        Mode = mode;

        _client = new Lazy<ServiceBusClient>(CreateClient, LazyThreadSafetyMode.ExecutionAndPublication);
        _sender = new Lazy<ServiceBusSender>(() => _client.Value.CreateSender(_settings.SendEntity), LazyThreadSafetyMode.ExecutionAndPublication);
    }

    public EndpointMode Mode { get; }

    /// <summary>Service Bus has its own dead-letter queue per queue / subscription.</summary>
    public bool UsesNativeDeadLetter => _settings.UseNativeDeadLetter;

    public Task PostAsync(byte[] message, CancellationToken ct = default)
        => _sender.Value.SendMessageAsync(new ServiceBusMessage(message) { ContentType = "application/json" }, ct);

    public async Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct)
    {
        await _lifecycleLock.WaitAsync(ct);
        try
        {
            if (_processor is not null)
                throw new InvalidOperationException($"Azure Service Bus endpoint '{_name}' is already listening.");

            var options = new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                MaxConcurrentCalls = _settings.MaxConcurrentCalls,
                PrefetchCount = _settings.PrefetchCount,
                MaxAutoLockRenewalDuration = _settings.MaxAutoLockRenewalDuration,
                SubQueue = _settings.ReadDeadLetterQueue ? SubQueue.DeadLetter : SubQueue.None
            };

            var processor = _settings.QueueName is not null
                ? _client.Value.CreateProcessor(_settings.QueueName, options)
                : _client.Value.CreateProcessor(_settings.TopicName!, RequireSubscription(), options);

            processor.ProcessMessageAsync += args => OnMessageAsync(args, handler);
            processor.ProcessErrorAsync += OnErrorAsync;

            // Starts the receive loop in the background and returns; connection problems surface via ProcessErrorAsync.
            await processor.StartProcessingAsync(ct);
            _processor = processor;
            _logger.LogInformation("Azure Service Bus endpoint '{Name}' listening on '{Entity}'{SubQueue}",
                _name, processor.EntityPath, _settings.ReadDeadLetterQueue ? " (dead-letter queue)" : "");
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    /// <summary>Stops receiving and waits for in-flight handlers, so no handler runs after this returns.</summary>
    public async Task StopListeningAsync()
    {
        await _lifecycleLock.WaitAsync();
        try
        {
            if (_processor is null) return;
            try
            {
                await _processor.StopProcessingAsync();
            }
            finally
            {
                await _processor.DisposeAsync();
                _processor = null;
            }
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    public IHealthCheckResult HealthCheck()
    {
        try
        {
            if (_processor is { IsProcessing: false, IsClosed: false })
                return HealthCheckResult.Unhealthy($"Azure Service Bus endpoint '{_name}' is not processing messages");

            using var cts = new CancellationTokenSource(HealthCheckTimeout);
            ProbeAsync(cts.Token).GetAwaiter().GetResult();
            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Azure Service Bus endpoint '{Name}' health check failed", _name);
            return HealthCheckResult.Unhealthy(ex.Message);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopListeningAsync();
        if (_sender.IsValueCreated)
            await _sender.Value.DisposeAsync();
        if (_client.IsValueCreated)
            await _client.Value.DisposeAsync();
        _lifecycleLock.Dispose();
    }

    private async Task OnMessageAsync(ProcessMessageEventArgs args, Func<byte[], CancellationToken, Task<ProcessResult>> handler)
    {
        var message = args.Message;
        ProcessResult result;
        try
        {
            result = await handler(message.Body.ToArray(), args.CancellationToken);
        }
        catch (OperationCanceledException) when (args.CancellationToken.IsCancellationRequested)
        {
            // Shutting down: leave the message unsettled; its lock expires and Service Bus redelivers it.
            return;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unhandled error dispatching message {MessageId} on endpoint '{Name}'", message.MessageId, _name);
            result = ProcessResult.Retry(ex);
        }

        try
        {
            await ServiceBusSettlement.SettleAsync(result, _settings.ReadDeadLetterQueue, new ProcessMessageEventArgsSettler(args),
                _logger, _name, message.MessageId, message.DeliveryCount, args.CancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Typically a lost lock: the message is redelivered after the lock expires.
            _logger.LogError(ex, "Could not settle message {MessageId} ({Outcome}) on endpoint '{Name}'; it will be redelivered when its lock expires",
                message.MessageId, result.Outcome, _name);
        }
    }

    private Task OnErrorAsync(ProcessErrorEventArgs args)
    {
        _logger.LogError(args.Exception, "Azure Service Bus error on endpoint '{Name}' ({ErrorSource}, entity '{EntityPath}')",
            _name, args.ErrorSource, args.EntityPath);
        return Task.CompletedTask;
    }

    private async Task ProbeAsync(CancellationToken ct)
    {
        var options = new ServiceBusReceiverOptions { SubQueue = _settings.ReadDeadLetterQueue ? SubQueue.DeadLetter : SubQueue.None };
        await using var receiver = _settings.QueueName is not null
            ? _client.Value.CreateReceiver(_settings.QueueName, options)
            : _settings.SubscriptionName is not null
                ? _client.Value.CreateReceiver(_settings.TopicName!, _settings.SubscriptionName, options)
                : null;

        if (receiver is not null)
        {
            _ = await receiver.PeekMessageAsync(cancellationToken: ct);
            return;
        }

        // Send-only topic endpoint: opening the sender link proves the namespace and topic are reachable.
        using var batch = await _sender.Value.CreateMessageBatchAsync(ct);
    }

    private string RequireSubscription()
        => _settings.SubscriptionName
           ?? throw new InvalidOperationException($"Azure Service Bus endpoint '{_name}' needs a SubscriptionName to receive from topic '{_settings.TopicName}'.");

    private ServiceBusClient CreateClient()
        => _settings.ConnectionString is not null
            ? new ServiceBusClient(_settings.ConnectionString)
            : new ServiceBusClient(_settings.FullyQualifiedNamespace, _settings.Credential);
}
