namespace NymBroker.RabbitMq;

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
}
