namespace MessageBroker.Core.Factory.Configuration;

public sealed class BrokerConfiguration
{
    public List<EndPointConfiguration> Endpoints { get; set; } = [];
}
