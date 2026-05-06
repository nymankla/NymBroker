namespace MessageBroker.Core.Factory.Configuration;

public sealed class TopicConfiguration
{
    public string TopicName { get; set; } = string.Empty;

    /// <summary>Message type name resolved via <see cref="MessageBroker.Core.Impl.MessageTypeRegistry"/>. Null matches any type.</summary>
    public string? MessageType { get; set; }

    public List<string> SubscriberEndpoints { get; set; } = [];
}
