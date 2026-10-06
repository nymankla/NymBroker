using System.Text.Json;
using NymBroker.Core.Message;

namespace NymBroker.Core.Route;

public sealed class JsonRouteCondition(Func<JsonElement, bool> predicate) : IRouteCondition
{
    public bool Evaluate(IMessageContext context, JsonElement messageElement) => predicate(messageElement);
}

public sealed class FromRouteCondition(string sourceEndpoint) : IRouteCondition
{
    public bool Evaluate(IMessageContext context, JsonElement messageElement)
        => string.Equals(context.Address?.From, sourceEndpoint, StringComparison.OrdinalIgnoreCase);
}

public sealed class NotFromRouteCondition(string sourceEndpoint) : IRouteCondition
{
    public bool Evaluate(IMessageContext context, JsonElement messageElement)
        => !string.Equals(context.Address?.From, sourceEndpoint, StringComparison.OrdinalIgnoreCase);
}

public sealed class MessageAgeRouteCondition(TimeSpan age) : IRouteCondition
{
    public bool Evaluate(IMessageContext context, JsonElement messageElement)
        => context.Created <= DateTime.UtcNow.Subtract(age);
}

public sealed class AndRouteCondition(IRouteCondition lhs, IRouteCondition rhs) : IRouteCondition
{
    public bool Evaluate(IMessageContext context, JsonElement messageElement)
        => lhs.Evaluate(context, messageElement) && rhs.Evaluate(context, messageElement);
}

public sealed class OrRouteCondition(IRouteCondition lhs, IRouteCondition rhs) : IRouteCondition
{
    public bool Evaluate(IMessageContext context, JsonElement messageElement)
        => lhs.Evaluate(context, messageElement) || rhs.Evaluate(context, messageElement);
}

/// <summary>Helpers for building condition chains in the fluent route and topic builders.</summary>
internal static class RouteConditions
{
    /// <summary>AND-s two optional conditions; null means "no condition".</summary>
    internal static IRouteCondition? Combine(IRouteCondition? existing, IRouteCondition? added)
        => existing is null ? added
         : added is null ? existing
         : new AndRouteCondition(existing, added);
}
