using System.Text.Json;
using MessageBroker.Core.Message;

namespace MessageBroker.Core.Route;

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
    public IRouteBuilder<T> When(Func<JsonElement, bool> condition) { _condition = new JsonRouteCondition(condition); return this; }
    public IRouteBuilder<T> WhenFrom(string sourceEndpoint) { _source = sourceEndpoint; return this; }
    public IRouteBuilder<T> WhenNotFrom(string sourceEndpoint) { _excludedSource = sourceEndpoint; return this; }
    public IRouteBuilder<T> WhenMessageIsOlderThan(TimeSpan age) { _condition = new MessageAgeRouteCondition(age); return this; }
    public IRouteBuilder<T> And(IRouteCondition lhs, IRouteCondition rhs) { _condition = new AndRouteCondition(lhs, rhs); return this; }
    public IRouteBuilder<T> Or(IRouteCondition lhs, IRouteCondition rhs) { _condition = new OrRouteCondition(lhs, rhs); return this; }
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
        routeContext.Condition = _condition ?? routeContext.Condition;
        routeContext.Transform = _transform ?? routeContext.Transform;

        _register(routeContext);
        return routeContext;
    }
}
