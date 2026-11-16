using System.Text.Json;
using NymBroker.Core.Message;

namespace NymBroker.Core.Route;

public sealed class JsonRouteCondition(Func<JsonElement, bool> predicate) : IRouteCondition, IDescribedRouteCondition
{
    public bool Evaluate(IMessageContext context, JsonElement messageElement) => predicate(messageElement);

    string IDescribedRouteCondition.Describe() => "predicate";
}

public sealed class FromRouteCondition(string sourceEndpoint) : IRouteCondition, IDescribedRouteCondition
{
    public bool Evaluate(IMessageContext context, JsonElement messageElement)
        => string.Equals(context.Address?.From, sourceEndpoint, StringComparison.OrdinalIgnoreCase);

    string IDescribedRouteCondition.Describe() => $"from {sourceEndpoint}";
}

public sealed class NotFromRouteCondition(string sourceEndpoint) : IRouteCondition, IDescribedRouteCondition
{
    public bool Evaluate(IMessageContext context, JsonElement messageElement)
        => !string.Equals(context.Address?.From, sourceEndpoint, StringComparison.OrdinalIgnoreCase);

    string IDescribedRouteCondition.Describe() => $"not from {sourceEndpoint}";
}

public sealed class MessageAgeRouteCondition(TimeSpan age) : IRouteCondition, IDescribedRouteCondition
{
    public bool Evaluate(IMessageContext context, JsonElement messageElement)
        => context.Created <= DateTime.UtcNow.Subtract(age);

    string IDescribedRouteCondition.Describe() => $"older than {age}";
}

public sealed class AndRouteCondition(IRouteCondition lhs, IRouteCondition rhs) : IRouteCondition, IDescribedRouteCondition
{
    public bool Evaluate(IMessageContext context, JsonElement messageElement)
        => lhs.Evaluate(context, messageElement) && rhs.Evaluate(context, messageElement);

    string IDescribedRouteCondition.Describe() => $"{RouteConditions.Describe(lhs)} and {RouteConditions.Describe(rhs)}";
}

public sealed class OrRouteCondition(IRouteCondition lhs, IRouteCondition rhs) : IRouteCondition, IDescribedRouteCondition
{
    public bool Evaluate(IMessageContext context, JsonElement messageElement)
        => lhs.Evaluate(context, messageElement) || rhs.Evaluate(context, messageElement);

    string IDescribedRouteCondition.Describe() => $"({RouteConditions.Describe(lhs)} or {RouteConditions.Describe(rhs)})";
}

/// <summary>A built-in condition that can say what it checks, for <see cref="INymBroker.Describe"/>.</summary>
internal interface IDescribedRouteCondition
{
    string Describe();
}

/// <summary>Helpers for building condition chains in the fluent route and topic builders.</summary>
internal static class RouteConditions
{
    /// <summary>AND-s two optional conditions; null means "no condition".</summary>
    internal static IRouteCondition? Combine(IRouteCondition? existing, IRouteCondition? added)
        => existing is null ? added
         : added is null ? existing
         : new AndRouteCondition(existing, added);

    /// <summary>The condition as text; a custom <see cref="IRouteCondition"/> is shown by its type name.</summary>
    internal static string Describe(IRouteCondition condition)
        => condition is IDescribedRouteCondition described ? described.Describe() : condition.GetType().Name;
}
