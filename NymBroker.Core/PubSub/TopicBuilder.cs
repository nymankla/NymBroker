using System.Collections.Immutable;
using System.Text.Json;
using NymBroker.Core.Consume;
using NymBroker.Core.Factory;
using NymBroker.Core.Message;
using NymBroker.Core.Route;
using Microsoft.Extensions.DependencyInjection;

namespace NymBroker.Core.PubSub;

internal sealed class TopicBuilder<T> : ITopicBuilder<T> where T : class
{
    private readonly string _topicName;
    private readonly Action<TopicContext> _register;
    private readonly NymBrokerBuilder _parent;
    private ImmutableList<string> _subscriberEndpoints = ImmutableList<string>.Empty;
    private ImmutableList<(Type, string)> _subscriberDispatchers = ImmutableList<(Type, string)>.Empty;
    private IRouteCondition? _condition;

    public TopicBuilder(string topicName, Action<TopicContext> register, NymBrokerBuilder parent)
    {
        _topicName = topicName;
        _register = register;
        _parent = parent;
    }

    public ITopicBuilder<T> SubscribeTo(string endpointName)
    {
        _subscriberEndpoints = _subscriberEndpoints.Add(endpointName);
        return this;
    }

    public ITopicBuilder<T> SubscribeWith<TSub>() where TSub : class, ISubscribe<T>
    {
        _subscriberDispatchers = _subscriberDispatchers.Add((typeof(TSub), typeof(TSub).Name));
        _parent.Services.AddKeyedTransient(typeof(IMessageSubscriber), typeof(TSub).Name, typeof(TSub));
        return this;
    }

    public ITopicBuilder<T> When(IRouteCondition condition) { _condition = condition; return this; }
    public ITopicBuilder<T> When(Func<JsonElement, bool> condition) { _condition = new JsonRouteCondition(condition); return this; }

    public NymBrokerBuilder Build()
    {
        _register(new TopicContext
        {
            TopicName = _topicName,
            MessageType = typeof(T),
            Condition = _condition,
            SubscriberEndpoints = _subscriberEndpoints,
            SubscriberDispatchers = _subscriberDispatchers
        });
        return _parent;
    }
}
