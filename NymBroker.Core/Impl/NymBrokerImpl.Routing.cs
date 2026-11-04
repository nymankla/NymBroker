using NymBroker.Core.Message;
using NymBroker.Core.Route;

namespace NymBroker.Core.Impl;

internal sealed partial class NymBrokerImpl
{
    public IRouteBuilder<T> Route<T>() where T : class
    {
        _messageTypeRegistry.Register(typeof(T));
        return new RouteBuilder<T>(AddRoute);
    }

    public IRouteBuilder<IAnyMessage> Route()
        => Route<IAnyMessage>();

    internal void AddRoute(RouteContext route)
    {
        _messageTypeRegistry.Register(route.MessageType);
        _routes = _routes.Add(route);
    }

    public RouteContext Route(IRouteBuilder routeBuilder)
    {
        ArgumentNullException.ThrowIfNull(routeBuilder);
        var routeContext = routeBuilder.Build();
        AddRoute(routeContext);
        return routeContext;
    }

    public IRouteBuilder<IAnyMessage> Route(Func<RouteContext> routeContextFactory)
    {
        ArgumentNullException.ThrowIfNull(routeContextFactory);
        return new RouteBuilder<IAnyMessage>(AddRoute, routeContextFactory);
    }
}
