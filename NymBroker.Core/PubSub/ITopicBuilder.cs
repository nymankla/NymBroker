using System.Text.Json;
using NymBroker.Core.Consume;
using NymBroker.Core.Factory;
using NymBroker.Core.Route;

namespace NymBroker.Core.PubSub;

public interface ITopicBuilder<T> where T : class
{
    ITopicBuilder<T> SubscribeTo(string endpointName);
    ITopicBuilder<T> SubscribeWith<TSub>() where TSub : class, ISubscribe<T>;
    ITopicBuilder<T> When(IRouteCondition condition);
    ITopicBuilder<T> When(Func<JsonElement, bool> condition);
    NymBrokerBuilder Build();
}
