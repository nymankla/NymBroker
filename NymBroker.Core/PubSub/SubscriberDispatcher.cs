using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Linq.Expressions;
using NymBroker.Core.Consume;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Message;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace NymBroker.Core.PubSub;

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
        List<Exception>? failures = null;
        foreach (var (subscriberType, serviceKey) in subscribers)
        {
            var invoked = false;
            try
            {
                var subscriber = scope.ServiceProvider.GetRequiredKeyedService<IMessageSubscriber>(serviceKey);
                var dispatch = DispatchCache.GetOrAdd(subscriberType, BuildDispatcher);
                invoked = true;
                await dispatch(subscriber, message, context, ct);
                RecordConsumed(context, message, serviceKey, "success");
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                if (invoked)
                    RecordConsumed(context, message, serviceKey, "failure");
                logger.LogError(ex, "Subscriber {Subscriber} failed processing message type {MessageType}",
                    subscriberType.Name, message.GetType().Name);
                (failures ??= []).Add(ex);
            }
        }

        if (failures != null)
            throw new AggregateException("One or more topic subscribers failed.", failures);
    }

    private static void RecordConsumed(IMessageContext context, object message, string serviceKey, string outcome)
    {
        var tags = new TagList
        {
            { "source", context.Address?.From ?? "unknown" },
            { "message_type", MessageTypeName.Get(message.GetType()) },
            { "consumer", serviceKey },
            { "kind", "subscriber" },
            { "outcome", outcome }
        };
        NymBrokerDiagnostics.MessagesConsumed.Add(1, tags);
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
