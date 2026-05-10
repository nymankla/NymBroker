using NymBroker.Core.Filter;
using NymBroker.Core.Message;
using NymBroker.Core.Route;

namespace NymBroker.Core.Impl;

public interface INymBroker
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

    INymBroker AddScheduledAction(TimeSpan timeSpan, Action action);
    INymBroker AddScheduledAction<T1>(TimeSpan timeSpan, Action<T1> action, T1 param1);
    INymBroker AddScheduledAction<T1, T2>(TimeSpan timeSpan, Action<T1, T2> action, T1 param1, T2 param2);
    INymBroker AddScheduledAction<T1>(string expression, Action<T1> action, T1 param1);

    INymBroker AddFilter(IMessageFilter filter);

    Task StartAsync(CancellationToken ct = default);
    Task StopAsync(CancellationToken ct = default);

    /// <summary>Publish a message to all topics matching its CLR type.</summary>
    Task PublishAsync<T>(T message, CancellationToken ct = default) where T : class;

    /// <summary>Publish a message directly to a named topic, bypassing type-based topic matching.</summary>
    Task PublishAsync<T>(string topicName, T message, CancellationToken ct = default) where T : class;

    /// <summary>Process raw UTF-8 JSON bytes arriving from an endpoint. Called by endpoint listeners.</summary>
    Task ProcessAsync(byte[] raw, string? sourceEndpoint = null, CancellationToken ct = default);

    /// <summary>Convenience overload for tests and external callers; converts the string to UTF-8 bytes.</summary>
    Task ProcessAsync(string raw, string? sourceEndpoint = null, CancellationToken ct = default);
}
