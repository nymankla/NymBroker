namespace NymBroker.Core.Factory.Configuration;

public sealed class BrokerConfiguration
{
    public List<EndPointConfiguration> Endpoints { get; set; } = [];
    public List<TopicConfiguration> Topics { get; set; } = [];
}
