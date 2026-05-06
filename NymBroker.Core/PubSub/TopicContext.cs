using System.Collections.Immutable;
using System.Text.Json;
using NymBroker.Core.Message;
using NymBroker.Core.Route;

namespace NymBroker.Core.PubSub;

public sealed class TopicContext
{
    public string TopicName { get; set; } = string.Empty;
    public Type MessageType { get; set; } = typeof(IAnyMessage);
    public IRouteCondition? Condition { get; set; }
    public ImmutableList<string> SubscriberEndpoints { get; set; } = ImmutableList<string>.Empty;
    public ImmutableList<(Type SubscriberType, string ServiceKey)> SubscriberDispatchers { get; set; }
        = ImmutableList<(Type, string)>.Empty;

    public bool Evaluate(Type messageType, IMessageContext context, JsonElement messageElement)
    {
        if (MessageType != typeof(IAnyMessage) && MessageType != messageType) return false;
        if (Condition != null && !Condition.Evaluate(context, messageElement)) return false;
        return true;
    }
}
