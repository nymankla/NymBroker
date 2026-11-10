using System.Text.Json;
using NymBroker.Core.Message;

namespace NymBroker.Core.Route;

public class RouteContext
{
    /// <summary>Message type this route applies to. Use <see cref="IAnyMessage"/> to match all types.</summary>
    public Type MessageType { get; set; } = typeof(IAnyMessage);

    public string DestinationEndpoint { get; set; } = string.Empty;

    /// <summary>When set, only matches messages that arrived from this source endpoint.</summary>
    public string? SourceEndpoint { get; set; }

    /// <summary>When set, excludes messages that arrived from this source endpoint.</summary>
    public string? ExcludedSourceEndpoint { get; set; }

    /// <summary>Optional predicate evaluated against the message payload element.</summary>
    public IRouteCondition? Condition { get; set; }

    public virtual bool Evaluate(Type messageType, IMessageContext context, JsonElement messageElement)
        => MatchesType(messageType)
           && MatchesSource(context.Address?.From)
           && (Condition == null || Condition.Evaluate(context, messageElement));

    private bool MatchesType(Type messageType) => MessageType == typeof(IAnyMessage) || MessageType == messageType;

    /// <summary>The source filter: <see cref="SourceEndpoint"/> must match, <see cref="ExcludedSourceEndpoint"/> must not.</summary>
    private bool MatchesSource(string? from)
        => (SourceEndpoint == null || string.Equals(SourceEndpoint, from, StringComparison.OrdinalIgnoreCase))
           && (ExcludedSourceEndpoint == null || !string.Equals(ExcludedSourceEndpoint, from, StringComparison.OrdinalIgnoreCase));
}
