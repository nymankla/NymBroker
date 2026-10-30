using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Resilience;

namespace NymBroker.Endpoint.AwsSqs;

/// <summary>
/// Amazon SQS queue endpoint (standard or FIFO). Pull-based: a long-polling loop receives up to
/// <see cref="AwsSqsSettings.MaxNumberOfMessages"/> messages, calls the broker for each, then settles the batch by the
/// returned <see cref="ProcessResult"/>s â€” Completed â†’ deleted (one DeleteMessageBatch); Retry â†’ visible again after
/// <see cref="AwsSqsSettings.RetryDelay"/>, so the queue's redrive policy dead-letters a message that keeps failing.
/// SQS cannot dead-letter a message on request, so <see cref="UsesNativeDeadLetter"/> is false and the broker's
/// dead-letter endpoint takes messages that can never succeed. The SQS client is thread-safe and long-lived; transient
/// faults are retried by the SDK, a failing receive by a backoff <see cref="RetryPolicy"/>.
/// </summary>
public sealed class AwsSqsEndPoint : IEndPointEventDriven, IAsyncDisposable
{
    private readonly string _name;
    private readonly AwsSqsSettings _settings;
    private readonly ILogger<AwsSqsEndPoint> _logger;
    private readonly IAmazonSQS _client;
    private readonly RetryPolicy _receivePolicy;
    private readonly SemaphoreSlim _lifecycleLock = new(1, 1);

    private string? _queueUrl;
    private CancellationTokenSource? _listeningCts;
    private Task? _loop;
    private volatile bool _loopFailed;

    public AwsSqsEndPoint(string name, AwsSqsSettings settings, ILogger<AwsSqsEndPoint> logger, EndpointMode mode = EndpointMode.ReadWrite)
        : this(name, settings, logger, mode, CreateClient(settings, name))
    {
    }

    internal AwsSqsEndPoint(string name, AwsSqsSettings settings, ILogger<AwsSqsEndPoint> logger, EndpointMode mode, IAmazonSQS client)
    {
        settings.Validate(name);
        _name = name;
        _settings = settings;
        _logger = logger;
        _client = client;
        Mode = mode;
        _queueUrl = settings.QueueUrl;

        _receivePolicy = new RetryPolicy(new RetryOptions
        {
            MaxRetryAttempts = int.MaxValue,
            Delay = TimeSpan.FromSeconds(1),
            MaxDelay = TimeSpan.FromSeconds(30),
            BackoffType = RetryBackoffType.Exponential,
            UseJitter = true,
            OnRetry = args =>
            {
                _logger.LogError(args.Exception, "AWS SQS endpoint '{Name}' receive failed (attempt {Attempt}); retrying in {Delay}",
                    _name, args.AttemptNumber + 1, args.Delay);
                return ValueTask.CompletedTask;
            }
        });
    }

    public EndpointMode Mode { get; }

    /// <summary>False: SQS has no "dead-letter this message" call; failures go to the broker's dead-letter endpoint.</summary>
    public bool UsesNativeDeadLetter => false;

    public async Task PostAsync(byte[] message, CancellationToken ct = default)
    {
        var (body, isBase64) = SqsWireFormat.Encode(message);
        var request = new SendMessageRequest
        {
            QueueUrl = await GetQueueUrlAsync(ct),
            MessageBody = body,
            MessageAttributes = SqsWireFormat.Attributes(isBase64)
        };
        if (_settings.IsFifo)
            (request.MessageGroupId, request.MessageDeduplicationId) = SqsWireFormat.FifoIds(message, _settings.MessageGroupId);

        await _client.SendMessageAsync(request, ct);
    }

    /// <summary>
    /// Sends in SendMessageBatch requests of up to 10 messages, in order. <b>Not atomic</b>: SQS accepts or rejects each
    /// entry separately. If an entry fails, the remaining requests are not sent and an <see cref="InvalidOperationException"/>
    /// lists the failed entries; the messages before them (and the other entries of that request) were sent.
    /// </summary>
    public async Task PostBatchAsync(IReadOnlyList<byte[]> messages, CancellationToken ct = default)
    {
        if (messages.Count == 0) return;
        var queueUrl = await GetQueueUrlAsync(ct);
        var sent = 0;

        foreach (var chunk in SqsWireFormat.Chunk(messages))
        {
            var entries = new List<SendMessageBatchRequestEntry>(chunk.Count);
            for (var i = 0; i < chunk.Count; i++)
            {
                var (original, body, isBase64) = chunk[i];
                var entry = new SendMessageBatchRequestEntry(i.ToString(), body) { MessageAttributes = SqsWireFormat.Attributes(isBase64) };
                if (_settings.IsFifo)
                    (entry.MessageGroupId, entry.MessageDeduplicationId) = SqsWireFormat.FifoIds(original, _settings.MessageGroupId);
                entries.Add(entry);
            }

            var response = await _client.SendMessageBatchAsync(new SendMessageBatchRequest(queueUrl, entries), ct);
            if (response.Failed is { Count: > 0 } failed)
            {
                var details = string.Join("; ", failed.Select(f => $"message {sent + int.Parse(f.Id)}: {f.Code} {f.Message}"));
                throw new InvalidOperationException(
                    $"AWS SQS endpoint '{_name}' could not send {failed.Count} of {messages.Count} message(s); later messages were not sent. {details}");
            }
            sent += chunk.Count;
        }
    }

    public async Task StartListeningAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken ct)
    {
        await _lifecycleLock.WaitAsync(ct);
        try
        {
            if (_loop is not null)
                throw new InvalidOperationException($"AWS SQS endpoint '{_name}' is already listening.");

            // Our own token: the one passed in is the broker's startup token, which stopping does not cancel.
            _listeningCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var token = _listeningCts.Token;
            _loopFailed = false;
            _loop = Task.Run(() => RunListenerAsync(handler, token), CancellationToken.None);
            _logger.LogInformation("AWS SQS endpoint '{Name}' listening on '{Queue}'", _name, _settings.DisplayName);
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    /// <summary>
    /// Stops receiving and waits for the batch in progress: handled messages are settled, messages not yet handled are
    /// made visible again at once, so no handler runs after this returns.
    /// </summary>
    public async Task StopListeningAsync()
    {
        await _lifecycleLock.WaitAsync();
        try
        {
            if (_loop is null) return;
            _listeningCts!.Cancel();
            await _loop;
            _loop = null;
            _listeningCts.Dispose();
            _listeningCts = null;
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    /// <summary>Blocking form for callers of the synchronous API; the broker calls <see cref="HealthCheckAsync"/>.</summary>
    public IHealthCheckResult HealthCheck() => HealthCheckAsync().GetAwaiter().GetResult();

    public Task<IHealthCheckResult> HealthCheckAsync(CancellationToken cancellationToken = default)
    {
        if (_loopFailed)
            return Task.FromResult(HealthCheckResult.Unhealthy($"AWS SQS endpoint '{_name}' listener stopped unexpectedly"));

        return HealthCheckResult.FromProbeAsync("AWS SQS", _name, _logger, async ct =>
            await _client.GetQueueAttributesAsync(new GetQueueAttributesRequest
            {
                QueueUrl = await GetQueueUrlAsync(ct),
                AttributeNames = ["ApproximateNumberOfMessages"]
            }, ct), cancellationToken: cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await StopListeningAsync();
        _client.Dispose();
        _lifecycleLock.Dispose();
    }

    private async Task RunListenerAsync(Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken token)
    {
        try
        {
            var request = new ReceiveMessageRequest
            {
                QueueUrl = await _receivePolicy.ExecuteAsync(async t => await GetQueueUrlAsync(t), token),
                MaxNumberOfMessages = _settings.MaxNumberOfMessages,
                WaitTimeSeconds = _settings.WaitTimeSeconds,
                VisibilityTimeout = _settings.VisibilityTimeout is { } v ? (int)v.TotalSeconds : null,
                MessageAttributeNames = [SqsWireFormat.EncodingAttribute],
                MessageSystemAttributeNames = ["ApproximateReceiveCount"]
            };

            while (!token.IsCancellationRequested)
            {
                var response = await _receivePolicy.ExecuteAsync(async t => await _client.ReceiveMessageAsync(request, t), token);
                if (response.Messages is { Count: > 0 } messages)   // empty: the long poll timed out, receive again
                    await HandleBatchAsync(request.QueueUrl, messages, handler, token);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _loopFailed = true;
            _logger.LogCritical(ex, "AWS SQS endpoint '{Name}' listener loop terminated unexpectedly", _name);
        }
    }

    private async Task HandleBatchAsync(string queueUrl, List<Message> messages,
        Func<byte[], CancellationToken, Task<ProcessResult>> handler, CancellationToken token)
    {
        var outcomes = new ProcessOutcome?[messages.Count];   // null: not handled (stopping)

        await Parallel.ForAsync(0, messages.Count, new ParallelOptions { MaxDegreeOfParallelism = _settings.MaxConcurrentCalls },
            async (i, _) =>
            {
                if (token.IsCancellationRequested) return;
                var message = messages[i];
                ProcessResult result;
                try
                {
                    result = await EndpointHandler.InvokeAsync(handler, SqsWireFormat.Decode(message), _logger, _name, token, message.MessageId);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    return;   // the handler saw the stop: leave the message for redelivery
                }

                if (result.Outcome != ProcessOutcome.Completed)
                {
                    // A DeadLetter result only reaches here from a custom handler (the broker dead-letters itself for
                    // this endpoint); SQS can't dead-letter on request, so it is retried and redrive takes over.
                    _logger.LogWarning("Message {MessageId} on endpoint '{Name}' will be redelivered (receive {ReceiveCount}): {Failure}",
                        message.MessageId, _name, ReceiveCount(message), result.FailureText);
                }
                outcomes[i] = result.Outcome;
            });

        // Not the stop token: a handled message must still be settled, or a stop right after handling would deliver it again.
        await SettleAsync(queueUrl, messages, outcomes);
    }

    private async Task SettleAsync(string queueUrl, List<Message> messages, ProcessOutcome?[] outcomes)
    {
        var delete = new List<DeleteMessageBatchRequestEntry>();
        var retry = new List<ChangeMessageVisibilityBatchRequestEntry>();
        var release = new List<ChangeMessageVisibilityBatchRequestEntry>();

        for (var i = 0; i < messages.Count; i++)
        {
            var id = i.ToString();
            var receipt = messages[i].ReceiptHandle;
            switch (outcomes[i])
            {
                case ProcessOutcome.Completed: delete.Add(new DeleteMessageBatchRequestEntry(id, receipt)); break;
                case null: release.Add(new ChangeMessageVisibilityBatchRequestEntry(id, receipt) { VisibilityTimeout = 0 }); break;
                default: retry.Add(new ChangeMessageVisibilityBatchRequestEntry(id, receipt) { VisibilityTimeout = (int)_settings.RetryDelay.TotalSeconds }); break;
            }
        }

        if (delete.Count > 0)
            await SettleBatchAsync("delete", messages, async () =>
                (await _client.DeleteMessageBatchAsync(new DeleteMessageBatchRequest(queueUrl, delete), CancellationToken.None)).Failed);
        if (retry.Count > 0)
            await SettleBatchAsync("return for retry", messages, async () =>
                (await _client.ChangeMessageVisibilityBatchAsync(new ChangeMessageVisibilityBatchRequest(queueUrl, retry), CancellationToken.None)).Failed);
        if (release.Count > 0)
            await SettleBatchAsync("release on stop", messages, async () =>
                (await _client.ChangeMessageVisibilityBatchAsync(new ChangeMessageVisibilityBatchRequest(queueUrl, release), CancellationToken.None)).Failed);
    }

    private async Task SettleBatchAsync(string action, List<Message> messages, Func<Task<List<BatchResultErrorEntry>?>> call)
    {
        try
        {
            foreach (var failure in await call() ?? [])
                _logger.LogError("AWS SQS endpoint '{Name}' could not {Action} message {MessageId}: {Code} {Error}; it is redelivered when its visibility timeout expires",
                    _name, action, messages[int.Parse(failure.Id)].MessageId, failure.Code, failure.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "AWS SQS endpoint '{Name}' could not {Action} {Count} message(s); they are redelivered when their visibility timeout expires",
                _name, action, messages.Count);
        }
    }

    private async Task<string> GetQueueUrlAsync(CancellationToken ct)
        => _queueUrl ??= (await _client.GetQueueUrlAsync(new GetQueueUrlRequest(_settings.QueueName), ct)).QueueUrl;

    private static string? ReceiveCount(Message message)
        => message.Attributes?.GetValueOrDefault("ApproximateReceiveCount");

    private static AmazonSQSClient CreateClient(AwsSqsSettings settings, string name)
    {
        settings.Validate(name);
        var config = new AmazonSQSConfig();
        if (settings.ServiceUrl is not null)
        {
            config.ServiceURL = settings.ServiceUrl;
            config.AuthenticationRegion = settings.Region ?? "us-east-1";   // signing needs a region; local servers accept any
        }
        else if (settings.Region is not null)
            config.RegionEndpoint = RegionEndpoint.GetBySystemName(settings.Region);

        return settings.Credentials is not null ? new AmazonSQSClient(settings.Credentials, config) : new AmazonSQSClient(config);
    }
}
