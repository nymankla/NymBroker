namespace NymBroker.Endpoint.RabbitMq;

public sealed class RabbitMqSettings
{
    public string HostName { get; set; } = "localhost";
    public int Port { get; set; } = 5672;
    public string User { get; set; } = "guest";
    public string Password { get; set; } = "guest";
    public string VirtualHost { get; set; } = "/";
    public string ReadQueueName { get; set; } = string.Empty;
    public string WriteQueueName { get; set; } = string.Empty;

    /// <summary>Seconds before attempting reconnect after connection loss.</summary>
    public int ReconnectDelaySeconds { get; set; } = 5;

    /// <summary>
    /// ACK every N messages with multiple=true instead of one per message.
    /// Higher values reduce broker round-trips at the cost of larger redelivery
    /// windows on crash. Requires ConsumerDispatchConcurrency=1 (the default).
    /// </summary>
    public int BatchAckSize { get; set; } = 1;

    /// <summary>
    /// When true (default), a message whose handler fails after it was already redelivered is nacked
    /// with requeue=false (dead-lettered if the queue has a DLX) instead of being requeued forever.
    /// </summary>
    public bool RejectRedeliveredFailures { get; set; } = true;

    /// <summary>
    /// When true (default), failed messages are settled by RabbitMQ: consumer failures are requeued (see
    /// <see cref="RejectRedeliveredFailures"/>) and messages that can never succeed are rejected without requeue, so they
    /// reach the queue's dead-letter exchange. When false, the broker posts failures to its own dead-letter endpoint
    /// (<c>WithDeadLetterEndpoint</c>) and the message is acked.
    /// </summary>
    public bool UseNativeDeadLetter { get; set; } = true;
}
