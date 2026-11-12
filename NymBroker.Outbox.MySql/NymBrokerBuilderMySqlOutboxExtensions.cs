using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Factory;
using NymBroker.Core.Outbox;

namespace NymBroker.Outbox.MySql;

public static class NymBrokerBuilderMySqlOutboxExtensions
{
    /// <summary>
    /// Adds a transactional outbox in a MySQL / MariaDB table: inject <see cref="IOutbox"/> and post in your
    /// <c>MySqlTransaction</c>; a hosted dispatcher sends the messages once the transaction commits.
    /// </summary>
    /// <param name="configure">Dispatcher options: poll interval, batch size, lease, retries.</param>
    public static NymBrokerBuilder AddMySqlOutbox(this NymBrokerBuilder builder, MySqlOutboxSettings? settings = null, Action<OutboxOptions>? configure = null)
    {
        var s = settings ?? new MySqlOutboxSettings();
        s.Validate();

        builder.Services.RemoveAll<MySqlOutboxSettings>();
        builder.Services.AddSingleton(s);
        return builder.AddOutboxStore(sp => new MySqlOutboxStore(s, sp.GetRequiredService<ILogger<MySqlOutboxStore>>()), configure);
    }
}
