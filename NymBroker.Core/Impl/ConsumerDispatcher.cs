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

    /// <summary>Each message type with its consumer's DI key (the consumer class name), for <see cref="INymBroker.Describe"/>.</summary>
    internal IEnumerable<(Type MessageType, string ServiceKey)> Registrations => _consumers.Select(c => (c.Key, c.Value.ServiceKey));

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
        ImmutableInterlocked.Update(ref _consumers, static (consumers, add) => WithConsumer(consumers, add), (messageType, serviceKey, dispatcher));
    }

    private static ImmutableDictionary<Type, (string ServiceKey, Func<IMessageConsumer, object, IMessageContext, CancellationToken, Task>? Dispatcher)> WithConsumer(
        ImmutableDictionary<Type, (string ServiceKey, Func<IMessageConsumer, object, IMessageContext, CancellationToken, Task>? Dispatcher)> consumers,
        (Type MessageType, string ServiceKey, Func<IMessageConsumer, object, IMessageContext, CancellationToken, Task>? Dispatcher) add)
    {
        if (!consumers.TryGetValue(add.MessageType, out var existing)) return consumers.Add(add.MessageType, (add.ServiceKey, add.Dispatcher));
        if (existing.ServiceKey == add.ServiceKey) return consumers;
        throw Factory.NymBrokerBuilder.DuplicateConsumer(add.MessageType, existing.ServiceKey, add.ServiceKey);
    }

    public async Task DispatchAsync(Type messageType, object message, IMessageContext context, CancellationToken ct)
    {
        if (!_consumers.TryGetValue(messageType, out var registration))
        {
            logger.LogWarning("No consumer registered for message type '{MessageType}'", messageType.Name);
            return;
        }

        await using var scope = scopeFactory.CreateAsyncScope();
        var consumer = scope.ServiceProvider.GetRequiredKeyedService<IMessageConsumer>(registration.ServiceKey);
        var dispatcher = registration.Dispatcher ?? ReflectionDispatcher(messageType);
        var tags = ConsumedTags(messageType, context, registration.ServiceKey);
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

    private static TagList ConsumedTags(Type messageType, IMessageContext context, string serviceKey) => new()
    {
        { "source", context.Address?.From ?? "unknown" },
        { "message_type", MessageTypeName.Get(messageType) },
        { "consumer", serviceKey },
        { "kind", "consumer" }
    };

    /// <summary>The compiled dispatcher for a consumer registered through reflection-based discovery (not under Native AOT).</summary>
    private static Func<IMessageConsumer, object, IMessageContext, CancellationToken, Task> ReflectionDispatcher(Type messageType)
    {
        if (!RuntimeFeature.IsDynamicCodeSupported)
            throw new InvalidOperationException("Native AOT requires AddConsumer<TConsumer, TMessage>() for consumer dispatch.");
        return DispatchCache.GetOrAdd(messageType, BuildDispatcher);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2070", Justification = "This reflection fallback is used only when dynamic code is supported; Native AOT dispatch uses registered delegates.")]
    private static Func<IMessageConsumer, object, IMessageContext, CancellationToken, Task> BuildDispatcher(Type messageType)
        => DispatchDelegate.Compile<IMessageConsumer>(typeof(IConsume<>).MakeGenericType(messageType), nameof(IConsume<object>.ConsumeAsync));
}
