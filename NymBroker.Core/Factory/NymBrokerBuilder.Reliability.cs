using NymBroker.Core.Filter;
using NymBroker.Core.Idempotency;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace NymBroker.Core.Factory;

public sealed partial class NymBrokerBuilder
{
    /// <summary>Forward failed and expired messages to this endpoint.</summary>
    public NymBrokerBuilder WithDeadLetterEndpoint(string endpointName)
    {
        _deadLetterEndpoint = endpointName;
        return this;
    }

    /// <summary>Copy every incoming raw message to this endpoint before processing.</summary>
    public NymBrokerBuilder AddWireTap(string endpointName)
    {
        _wireTapEndpoints.Add(endpointName);
        return this;
    }

    /// <summary>Discard (and dead-letter if configured) messages older than maxAge.</summary>
    public NymBrokerBuilder DiscardMessagesOlderThan(TimeSpan maxAge)
    {
        _maxMessageAge = maxAge;
        return this;
    }

    /// <summary>Log every incoming message at Debug level before routing and consumer dispatch.</summary>
    public NymBrokerBuilder AddMessageLoggingFilter()
    {
        _services.AddSingleton<LoggingFilter>();
        _builderFilterTypes.Add(typeof(LoggingFilter));
        return this;
    }

    /// <summary>
    /// Register an idempotent receiver with the in-memory store: duplicate message IDs within the TTL window are
    /// dropped (per process). For a durable store shared by several instances use a database store package
    /// (e.g. <c>AddSqlServerIdempotency</c>) or <see cref="AddIdempotentReceiver(IIdempotencyStore)"/>.
    /// </summary>
    public NymBrokerBuilder AddIdempotentReceiver(TimeSpan? ttl = null)
        => AddIdempotentReceiver(new InMemoryIdempotencyStore(ttl ?? TimeSpan.FromHours(24)));

    /// <summary>Register an idempotent receiver backed by <paramref name="store"/>.</summary>
    public NymBrokerBuilder AddIdempotentReceiver(IIdempotencyStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        return AddIdempotentReceiver(_ => store);
    }

    /// <summary>Register an idempotent receiver backed by a store resolved from DI (e.g. registered by an extension package).</summary>
    public NymBrokerBuilder AddIdempotentReceiver<TStore>() where TStore : class, IIdempotencyStore
    {
        _services.TryAddSingleton<TStore>();
        return AddIdempotentReceiver(sp => sp.GetRequiredService<TStore>());
    }

    /// <summary>Register an idempotent receiver backed by the store <paramref name="factory"/> creates. One store per broker; the last registration wins.</summary>
    public NymBrokerBuilder AddIdempotentReceiver(Func<IServiceProvider, IIdempotencyStore> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        _services.RemoveAll<IIdempotencyStore>();
        _services.AddSingleton(factory);
        _idempotentReceiver = true;
        return this;
    }

    /// <summary>
    /// Register an idempotent receiver backed by a database store that needs periodic cleanup (what the
    /// <c>NymBroker.Idempotency.*</c> packages call). <paramref name="factory"/> creates the store once (singleton); when
    /// <paramref name="cleanupInterval"/> is positive a hosted service calls
    /// <see cref="IExpiringIdempotencyStore.DeleteExpiredAsync"/> on that interval, logging failures and retrying at the
    /// next one. One store per broker; the last registration wins.
    /// </summary>
    /// <param name="tableName">Where the entries live, for log messages.</param>
    public NymBrokerBuilder AddIdempotencyStore<TStore>(Func<IServiceProvider, TStore> factory, TimeSpan cleanupInterval, string tableName)
        where TStore : class, IExpiringIdempotencyStore
    {
        ArgumentNullException.ThrowIfNull(factory);

        _services.RemoveAll<TStore>();
        _services.AddSingleton(factory);
        AddIdempotentReceiver(sp => sp.GetRequiredService<TStore>());

        // Replace an earlier registration of the same store type, including its cleanup service.
        _services.RemoveAll<IdempotencyCleanupOptions<TStore>>();
        for (var i = _services.Count - 1; i >= 0; i--)
        {
            if (_services[i].ServiceType == typeof(IHostedService) && _services[i].ImplementationType == typeof(IdempotencyCleanupService<TStore>))
                _services.RemoveAt(i);
        }

        if (cleanupInterval > TimeSpan.Zero)
        {
            _services.AddSingleton(new IdempotencyCleanupOptions<TStore>(cleanupInterval, tableName));
            _services.AddSingleton<IHostedService, IdempotencyCleanupService<TStore>>();
        }
        return this;
    }

    // --- Health check ---
}
