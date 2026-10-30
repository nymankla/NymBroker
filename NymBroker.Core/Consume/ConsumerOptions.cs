namespace NymBroker.Core.Consume;

/// <summary>Options of a consumer (<c>NymBrokerBuilder.AddConsumer&lt;TConsumer&gt;(configure)</c>).</summary>
public sealed class ConsumerOptions
{
    /// <summary>
    /// Whether a message whose consumer throws is redelivered by its transport. Default true. False dead-letters it at
    /// once (reason <c>ConsumerFailed</c>) when it came from an endpoint with its own dead-letter queue (the SQL queue
    /// tables, RabbitMQ, Service Bus) instead of returning it for retry. Messages from other endpoints are posted to the
    /// broker's dead-letter endpoint either way, without a retry.
    /// </summary>
    public bool RetryOnFailure { get; set; } = true;
}
