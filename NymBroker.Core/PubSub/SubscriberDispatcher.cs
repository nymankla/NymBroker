using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using NymBroker.Core.Consume;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Impl;
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
                if (!RuntimeFeature.IsDynamicCodeSupported)
                    throw new InvalidOperationException("Topic subscriber dispatch is not yet supported in Native AOT applications.");
                var dispatch = DispatchCache.GetOrAdd(subscriberType, BuildDispatcher);
                invoked = true;
                await dispatch(subscriber, message, context, ct);
                RecordConsumed(context, message, serviceKey, "success");
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
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

    [UnconditionalSuppressMessage("Trimming", "IL2070", Justification = "This reflection fallback is used only when dynamic code is supported; Native AOT subscriber dispatch is explicitly rejected.")]
    private static Func<IMessageSubscriber, object, IMessageContext, CancellationToken, Task> BuildDispatcher(Type subscriberType)
        => DispatchDelegate.Compile<IMessageSubscriber>(
            subscriberType.GetInterfaces().First(static i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ISubscribe<>)),
            nameof(ISubscribe<object>.ReceiveAsync));
}
