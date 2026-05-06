using System.Text.Json;
using MessageBroker.Core.Consume;
using MessageBroker.Core.Factory;
using MessageBroker.Core.Route;

namespace MessageBroker.Core.PubSub;

public interface ITopicBuilder<T> where T : class
{
    ITopicBuilder<T> SubscribeTo(string endpointName);
    ITopicBuilder<T> SubscribeWith<TSub>() where TSub : class, ISubscribe<T>;
    ITopicBuilder<T> When(IRouteCondition condition);
    ITopicBuilder<T> When(Func<JsonElement, bool> condition);
    MessageBrokerBuilder Build();
}
