using System.Text.Json;
using MessageBroker.Core.Message;

namespace MessageBroker.Core.Route;

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
