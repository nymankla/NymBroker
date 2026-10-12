using System.Text.Json.Serialization;
using Amazon.Runtime;

namespace NymBroker.Endpoint.AwsSqs;

public sealed class AwsSqsSettings
{
    /// <summary>Full queue URL, e.g. <c>https://sqs.eu-north-1.amazonaws.com/123456789012/orders</c>. Use this <b>or</b> <see cref="QueueName"/>.</summary>
    public string? QueueUrl { get; set; }

    /// <summary>Queue name, resolved to its URL on first use. Use this <b>or</b> <see cref="QueueUrl"/>. A name ending in <c>.fifo</c> is a FIFO queue.</summary>
    public string? QueueName { get; set; }

    /// <summary>AWS region system name, e.g. <c>eu-north-1</c>. Unset: the SDK's default (AWS_REGION, profile).</summary>
    public string? Region { get; set; }

    /// <summary>Endpoint override for an SQS-compatible server such as ElasticMQ or LocalStack, e.g. <c>http://localhost:9324</c>.</summary>
    public string? ServiceUrl { get; set; }

    /// <summary>Explicit credentials. Code-only — not read from config files. Unset: the SDK's default chain (environment, profile, IAM role).</summary>
    [JsonIgnore]
    public AWSCredentials? Credentials { get; set; }

    /// <summary>Long-polling wait per receive call, 0–20 seconds. 20 (default) keeps empty receives, and their cost, low.</summary>
    public int WaitTimeSeconds { get; set; } = 20;

    /// <summary>Messages per receive call, 1–10.</summary>
    public int MaxNumberOfMessages { get; set; } = 10;

    /// <summary>
    /// How long received messages stay invisible to other consumers. Unset: the queue's own setting. It must cover handling
    /// a whole receive batch (<see cref="MaxNumberOfMessages"/> × handler time / <see cref="MaxConcurrentCalls"/>), because
    /// messages are deleted when the batch is done; a message whose visibility expires first is delivered again.
    /// </summary>
    public TimeSpan? VisibilityTimeout { get; set; }

    /// <summary>
    /// How long a message that failed (Retry) stays invisible before it is delivered again, 0 (default) to 12 hours. The
    /// queue's redrive policy (<c>maxReceiveCount</c>) moves a message that keeps failing to its dead-letter queue; without
    /// a redrive policy it is retried until it succeeds or expires.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.Zero;

    /// <summary>Messages of one receive batch handled in parallel. Values above 1 give up ordering (also within a FIFO group).</summary>
    public int MaxConcurrentCalls { get; set; } = 1;

    /// <summary>
    /// FIFO queues only: the message group of sent messages. Unset (default): the envelope's <c>correlationId</c>, so a
    /// conversation — and the parts of a split message — stay in order while unrelated messages are processed in parallel
    /// by other consumers.
    /// </summary>
    public string? MessageGroupId { get; set; }

    internal bool IsFifo => (QueueUrl ?? QueueName)!.EndsWith(".fifo", StringComparison.Ordinal);

    internal string DisplayName => QueueName ?? QueueUrl!;

    /// <summary>Throws <see cref="InvalidOperationException"/> describing the first invalid setting.</summary>
    internal void Validate(string endpointName)
    {
        void Fail(string message) => throw new InvalidOperationException($"AWS SQS endpoint '{endpointName}': {message}");

        if (string.IsNullOrWhiteSpace(QueueUrl) == string.IsNullOrWhiteSpace(QueueName))
            Fail("set either QueueUrl or QueueName, not both or neither.");
        if (QueueUrl is not null && !Uri.TryCreate(QueueUrl, UriKind.Absolute, out _))
            Fail($"QueueUrl '{QueueUrl}' is not an absolute URL.");
        if (ServiceUrl is not null && !Uri.TryCreate(ServiceUrl, UriKind.Absolute, out _))
            Fail($"ServiceUrl '{ServiceUrl}' is not an absolute URL.");
        if (WaitTimeSeconds is < 0 or > 20)
            Fail("WaitTimeSeconds must be between 0 and 20.");
        if (MaxNumberOfMessages is < 1 or > 10)
            Fail("MaxNumberOfMessages must be between 1 and 10.");
        if (VisibilityTimeout is { } v && (v < TimeSpan.Zero || v > TimeSpan.FromHours(12)))
            Fail("VisibilityTimeout must be between 0 and 12 hours.");
        if (RetryDelay < TimeSpan.Zero || RetryDelay > TimeSpan.FromHours(12))
            Fail("RetryDelay must be between 0 and 12 hours.");
        if (MaxConcurrentCalls < 1)
            Fail("MaxConcurrentCalls must be at least 1.");
        if (MessageGroupId is not null && !IsFifo)
            Fail("MessageGroupId only applies to FIFO queues (.fifo).");
    }
}
