using System.Collections.Concurrent;
using System.Linq.Expressions;
using MessageBroker.Core.Consume;
using MessageBroker.Core.Message;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace MessageBroker.Core.PubSub;

public sealed class SubscriberDispatcher(IServiceScopeFactory scopeFactory, ILogger<SubscriberDispatcher> logger)
{
    private static readonly ConcurrentDictionary<Type, Func<IMessageSubscriber, object, IMessageContext, CancellationToken, Task>> DispatchCache = new();

    public async Task DispatchAsync(
        IReadOnlyList<(Type SubscriberType, string ServiceKey)> subscribers,
        object message,
        IMessageContext context,
        CancellationToken ct)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        foreach (var (subscriberType, serviceKey) in subscribers)
        {
            try
            {
                var subscriber = scope.ServiceProvider.GetRequiredKeyedService<IMessageSubscriber>(serviceKey);
                var dispatch = DispatchCache.GetOrAdd(subscriberType, BuildDispatcher);
                await dispatch(subscriber, message, context, ct);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Subscriber {Subscriber} failed processing message type {MessageType}",
                    subscriberType.Name, message.GetType().Name);
            }
        }
    }

    private static Func<IMessageSubscriber, object, IMessageContext, CancellationToken, Task> BuildDispatcher(Type subscriberType)
    {
        var subscribeInterface = subscriberType.GetInterfaces()
            .First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ISubscribe<>));
        var messageType = subscribeInterface.GetGenericArguments()[0];

        var subscriberParam = Expression.Parameter(typeof(IMessageSubscriber), "subscriber");
        var messageParam = Expression.Parameter(typeof(object), "message");
        var contextParam = Expression.Parameter(typeof(IMessageContext), "context");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");

        var castSubscriber = Expression.Convert(subscriberParam, subscribeInterface);
        var castMessage = Expression.Convert(messageParam, messageType);
        var method = subscribeInterface.GetMethod(nameof(ISubscribe<object>.ReceiveAsync))!;
        var call = Expression.Call(castSubscriber, method, castMessage, contextParam, ctParam);

        return Expression.Lambda<Func<IMessageSubscriber, object, IMessageContext, CancellationToken, Task>>(
            call, subscriberParam, messageParam, contextParam, ctParam
        ).Compile();
    }
}
