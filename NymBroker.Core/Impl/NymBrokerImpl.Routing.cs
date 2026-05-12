using NymBroker.Core.Message;
using NymBroker.Core.Route;

namespace NymBroker.Core.Impl;

public sealed partial class NymBrokerImpl
{
    public IRouteBuilder<T> Route<T>() where T : class
    {
        _messageTypeRegistry.Register(typeof(T));
        return new RouteBuilder<T>(ctx =>
        {
            _messageTypeRegistry.Register(ctx.MessageType);
            _routes = _routes.Add(ctx);
        });
    }

    public IRouteBuilder<IAnyMessage> Route()
        => Route<IAnyMessage>();

    public RouteContext Route(IRouteBuilder routeBuilder)
    {
        ArgumentNullException.ThrowIfNull(routeBuilder);
        var routeContext = routeBuilder.Build();
        _messageTypeRegistry.Register(routeContext.MessageType);
        _routes = _routes.Add(routeContext);
        return routeContext;
    }

    public IRouteBuilder<IAnyMessage> Route(Func<RouteContext> routeContextFactory)
    {
        ArgumentNullException.ThrowIfNull(routeContextFactory);
        return new RouteBuilder<IAnyMessage>(ctx =>
        {
            _messageTypeRegistry.Register(ctx.MessageType);
            _routes = _routes.Add(ctx);
        }, routeContextFactory);
    }
}
