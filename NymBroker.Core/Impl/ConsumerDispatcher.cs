using System.Collections.Concurrent;
using System.Diagnostics;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using NymBroker.Core.Consume;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Message;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Impl;

public sealed class ConsumerDispatcher(IServiceScopeFactory scopeFactory, ILogger<ConsumerDispatcher> logger)
{
    private ImmutableDictionary<Type, (string ServiceKey, Func<IMessageConsumer, object, IMessageContext, CancellationToken, Task>? Dispatcher)> _consumers = ImmutableDictionary<Type, (string, Func<IMessageConsumer, object, IMessageContext, CancellationToken, Task>?)>.Empty;

    private static readonly ConcurrentDictionary<Type, Func<IMessageConsumer, object, IMessageContext, CancellationToken, Task>> DispatchCache = new();

    /// <summary>
    /// Registers the consumer (DI key) for a message type. Registering the same key again is a no-op; a different key
    /// for a type that already has a consumer throws, instead of silently replacing it.
    /// </summary>
    public void RegisterConsumer(Type messageType, string serviceKey)
        => RegisterConsumerWithDispatcher(messageType, serviceKey, null);

    internal void RegisterConsumerWithDispatcher(
        Type messageType,
        string serviceKey,
        Func<IMessageConsumer, object, IMessageContext, CancellationToken, Task>? dispatcher)
    {
        ImmutableInterlocked.Update(ref _consumers, consumers =>
        {
            if (consumers.TryGetValue(messageType, out var existing))
            {
                if (existing.ServiceKey == serviceKey) return consumers;
                throw Factory.NymBrokerBuilder.DuplicateConsumer(messageType, existing.ServiceKey, serviceKey);
            }
            return consumers.Add(messageType, (serviceKey, dispatcher));
        });
    }

    public async Task DispatchAsync(Type messageType, object message, IMessageContext context, CancellationToken ct)
    {
        if (!_consumers.TryGetValue(messageType, out var registration))
        {
            logger.LogWarning("No consumer registered for message type '{MessageType}'", messageType.Name);
            return;
        }

        var serviceKey = registration.ServiceKey;
        await using var scope = scopeFactory.CreateAsyncScope();
        var consumer = scope.ServiceProvider.GetRequiredKeyedService<IMessageConsumer>(serviceKey);
        var dispatcher = registration.Dispatcher;
        if (dispatcher is null)
        {
            if (!RuntimeFeature.IsDynamicCodeSupported)
                throw new InvalidOperationException("Native AOT requires AddConsumer<TConsumer, TMessage>() for consumer dispatch.");
            dispatcher = DispatchCache.GetOrAdd(messageType, BuildDispatcher);
        }
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
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
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

    [UnconditionalSuppressMessage("Trimming", "IL2070", Justification = "This reflection fallback is used only when dynamic code is supported; Native AOT dispatch uses registered delegates.")]
    private static Func<IMessageConsumer, object, IMessageContext, CancellationToken, Task> BuildDispatcher(Type messageType)
        => DispatchDelegate.Compile<IMessageConsumer>(typeof(IConsume<>).MakeGenericType(messageType), nameof(IConsume<object>.ConsumeAsync));
}
