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
}
