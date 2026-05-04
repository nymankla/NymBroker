using MessageBroker.Core.Filter;
using MessageBroker.Core.Message;
using MessageBroker.Core.Route;

namespace MessageBroker.Core.Impl;

public interface IMessageBroker
{
    /// <summary>Serialize and post a typed message to a named endpoint.</summary>
    Task PostAsync<T>(string endpointName, T message, CancellationToken ct = default) where T : class;

    /// <summary>Post a pre-serialized stream to a named endpoint.</summary>
    Task PostAsync(string endpointName, Stream messageStream, CancellationToken ct = default);

    /// <summary>Start a fluent route definition for message type T.</summary>
    IRouteBuilder<T> Route<T>() where T : class;

    /// <summary>Start a fluent route definition that matches any message type.</summary>
    IRouteBuilder<IAnyMessage> Route();

    /// <summary>Register a route from a custom route builder.</summary>
    RouteContext Route(IRouteBuilder routeBuilder);

    /// <summary>Start a route definition backed by a custom route context factory.</summary>
    IRouteBuilder<IAnyMessage> Route(Func<RouteContext> routeContextFactory);

    IMessageBroker AddScheduledAction(TimeSpan timeSpan, Action action);
    IMessageBroker AddScheduledAction<T1>(TimeSpan timeSpan, Action<T1> action, T1 param1);
    IMessageBroker AddScheduledAction<T1, T2>(TimeSpan timeSpan, Action<T1, T2> action, T1 param1, T2 param2);
    IMessageBroker AddScheduledAction<T1>(string expression, Action<T1> action, T1 param1);

    IMessageBroker AddFilter(IMessageFilter filter);

    Task StartAsync(CancellationToken ct = default);
    Task StopAsync(CancellationToken ct = default);

    /// <summary>Process a raw JSON string arriving from an endpoint. Called by endpoint listeners.</summary>
    Task ProcessAsync(string raw, string? sourceEndpoint = null, CancellationToken ct = default);
}
