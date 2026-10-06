using System.Text.Json;
using NymBroker.Core.Message;

namespace NymBroker.Core.Route;

internal sealed class RouteBuilder<T> : IRouteBuilder<T> where T : class
{
    private string? _destination;
    private string? _source;
    private string? _excludedSource;
    private string? _transform;
    private IRouteCondition? _condition;
    private readonly Action<RouteContext> _register;
    private readonly Func<RouteContext>? _factory;

    public RouteBuilder(Action<RouteContext> register, Func<RouteContext>? factory = null)
    {
        _register = register;
        _factory = factory;
    }

    public IRouteBuilder<T> To(string endpointName) { _destination = endpointName; return this; }
    // Content conditions accumulate: every When / WhenMessageIsOlderThan / And / Or is AND-ed with the ones before it,
    // so a fluent chain means "all of these".
    public IRouteBuilder<T> When(Func<JsonElement, bool> condition) => AddCondition(new JsonRouteCondition(condition));
    public IRouteBuilder<T> WhenFrom(string sourceEndpoint) { _source = sourceEndpoint; return this; }
    public IRouteBuilder<T> WhenNotFrom(string sourceEndpoint) { _excludedSource = sourceEndpoint; return this; }
    public IRouteBuilder<T> WhenMessageIsOlderThan(TimeSpan age) => AddCondition(new MessageAgeRouteCondition(age));
    public IRouteBuilder<T> And(IRouteCondition lhs, IRouteCondition rhs) => AddCondition(new AndRouteCondition(lhs, rhs));
    public IRouteBuilder<T> Or(IRouteCondition lhs, IRouteCondition rhs) => AddCondition(new OrRouteCondition(lhs, rhs));
    public IRouteBuilder<T> Transform(string fileName) { _transform = fileName; return this; }

    public RouteContext Build()
    {
        if (string.IsNullOrEmpty(_destination))
            throw new InvalidOperationException("Route must have a destination — call .To(endpointName) before .Build().");

        var routeContext = _factory?.Invoke() ?? new RouteContext { MessageType = typeof(T) };

        if (_factory != null && routeContext.MessageType == typeof(IAnyMessage) && typeof(T) != typeof(IAnyMessage))
            routeContext.MessageType = typeof(T);

        routeContext.DestinationEndpoint = _destination;
        routeContext.SourceEndpoint = _source ?? routeContext.SourceEndpoint;
        routeContext.ExcludedSourceEndpoint = _excludedSource ?? routeContext.ExcludedSourceEndpoint;
        // A condition already on a factory-created context is kept and AND-ed with the builder's conditions.
        routeContext.Condition = RouteConditions.Combine(routeContext.Condition, _condition);
        routeContext.Transform = _transform ?? routeContext.Transform;

        _register(routeContext);
        return routeContext;
    }

    private RouteBuilder<T> AddCondition(IRouteCondition condition)
    {
        _condition = RouteConditions.Combine(_condition, condition);
        return this;
    }
}
