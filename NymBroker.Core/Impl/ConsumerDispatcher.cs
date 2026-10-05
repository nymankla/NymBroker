using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Collections.Immutable;
using System.Linq.Expressions;
using NymBroker.Core.Consume;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Message;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Impl;

public sealed class ConsumerDispatcher(IServiceScopeFactory scopeFactory, ILogger<ConsumerDispatcher> logger)
{
    private ImmutableDictionary<Type, string> _consumerKeys = ImmutableDictionary<Type, string>.Empty;

    private static readonly ConcurrentDictionary<Type, Func<IMessageConsumer, object, IMessageContext, CancellationToken, Task>> DispatchCache = new();

    public void RegisterConsumer(Type messageType, string serviceKey)
    {
        _consumerKeys = _consumerKeys.SetItem(messageType, serviceKey);
    }

    public async Task DispatchAsync(Type messageType, object message, IMessageContext context, CancellationToken ct)
    {
        if (!_consumerKeys.TryGetValue(messageType, out var serviceKey))
        {
            logger.LogWarning("No consumer registered for message type '{MessageType}'", messageType.Name);
            return;
        }

        await using var scope = scopeFactory.CreateAsyncScope();
        var consumer = scope.ServiceProvider.GetRequiredKeyedService<IMessageConsumer>(serviceKey);
        var dispatcher = DispatchCache.GetOrAdd(messageType, BuildDispatcher);
        var tags = new TagList
        {
            { "source", context.Address?.From ?? "unknown" },
            { "message_type", MessageTypeName.Get(messageType) },
            { "consumer", serviceKey },
            { "kind", "consumer" }
        };
        try
        {
            await dispatcher(consumer, message, context, ct);
            tags.Add("outcome", "success");
            NymBrokerDiagnostics.MessagesConsumed.Add(1, tags);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            tags.Add("outcome", "failure");
            NymBrokerDiagnostics.MessagesConsumed.Add(1, tags);
            throw;
        }
    }

    private static Func<IMessageConsumer, object, IMessageContext, CancellationToken, Task> BuildDispatcher(Type messageType)
    {
        var consumerParam = Expression.Parameter(typeof(IMessageConsumer), "consumer");
        var messageParam = Expression.Parameter(typeof(object), "message");
        var contextParam = Expression.Parameter(typeof(IMessageContext), "context");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");

        var consumeInterface = typeof(IConsume<>).MakeGenericType(messageType);
        var castConsumer = Expression.Convert(consumerParam, consumeInterface);
        var castMessage = Expression.Convert(messageParam, messageType);
        var method = consumeInterface.GetMethod(nameof(IConsume<object>.ConsumeAsync))!;
        var call = Expression.Call(castConsumer, method, castMessage, contextParam, ctParam);

        return Expression.Lambda<Func<IMessageConsumer, object, IMessageContext, CancellationToken, Task>>(
            call, consumerParam, messageParam, contextParam, ctParam
        ).Compile();
    }
}
