using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NymBroker.Core.Outbox;

namespace NymBroker.Core.Factory;

public sealed partial class NymBrokerBuilder
{
    private bool _outbox;

    /// <summary>
    /// Adds a transactional outbox backed by the store <paramref name="factory"/> creates (what the <c>NymBroker.Outbox.*</c>
    /// packages call): registers <see cref="IOutbox"/>, and a hosted dispatcher that sends committed rows, started after
    /// the broker. One outbox per broker; the last registration wins.
    /// </summary>
    /// <exception cref="ArgumentException">An option set by <paramref name="configure"/> is out of range.</exception>
    public NymBrokerBuilder AddOutboxStore<TStore>(Func<IServiceProvider, TStore> factory, Action<OutboxOptions>? configure = null)
        where TStore : class, IOutboxStore
    {
        ArgumentNullException.ThrowIfNull(factory);
        var options = new OutboxOptions();
        configure?.Invoke(options);
        options.Validate();

        _services.RemoveAll<TStore>();
        _services.AddSingleton(factory);
        _services.RemoveAll<IOutboxStore>();
        _services.AddSingleton<IOutboxStore>(sp => sp.GetRequiredService<TStore>());
        _services.RemoveAll<OutboxOptions>();
        _services.AddSingleton(options);
        _services.TryAddSingleton<IOutbox, Outbox.Outbox>();
        _outbox = true;
        return this;
    }
}
