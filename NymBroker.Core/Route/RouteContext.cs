using System.Text.Json;
using NymBroker.Core.Message;

namespace NymBroker.Core.Route;

public class RouteContext
{
    /// <summary>Message type this route applies to. Use <see cref="IAnyMessage"/> to match all types.</summary>
    public Type MessageType { get; set; } = typeof(IAnyMessage);

    public string DestinationEndpoint { get; set; } = string.Empty;

    public string? Transform { get; set; }

    /// <summary>When set, only matches messages that arrived from this source endpoint.</summary>
    public string? SourceEndpoint { get; set; }

    /// <summary>When set, excludes messages that arrived from this source endpoint.</summary>
    public string? ExcludedSourceEndpoint { get; set; }

    /// <summary>Optional predicate evaluated against the message payload element.</summary>
    public IRouteCondition? Condition { get; set; }

    public virtual bool Evaluate(Type messageType, IMessageContext context, JsonElement messageElement)
    {
        if (MessageType != typeof(IAnyMessage) && MessageType != messageType) return false;
        if (SourceEndpoint != null && !string.Equals(SourceEndpoint, context.Address?.From, StringComparison.OrdinalIgnoreCase)) return false;
        if (ExcludedSourceEndpoint != null && string.Equals(ExcludedSourceEndpoint, context.Address?.From, StringComparison.OrdinalIgnoreCase)) return false;
        if (Condition != null && !Condition.Evaluate(context, messageElement)) return false;
        return true;
    }
}
