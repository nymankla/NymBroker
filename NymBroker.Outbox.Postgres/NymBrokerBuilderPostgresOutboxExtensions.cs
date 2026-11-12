using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Factory;
using NymBroker.Core.Outbox;

namespace NymBroker.Outbox.Postgres;

public static class NymBrokerBuilderPostgresOutboxExtensions
{
    /// <summary>
    /// Adds a transactional outbox in a PostgreSQL table: inject <see cref="IOutbox"/> and post in your
    /// <c>NpgsqlTransaction</c>; a hosted dispatcher sends the messages once the transaction commits.
    /// </summary>
    /// <param name="configure">Dispatcher options: poll interval, batch size, lease, retries.</param>
    public static NymBrokerBuilder AddPostgresOutbox(this NymBrokerBuilder builder, PostgresOutboxSettings settings, Action<OutboxOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();

        builder.Services.RemoveAll<PostgresOutboxSettings>();
        builder.Services.AddSingleton(settings);
        return builder.AddOutboxStore(sp => new PostgresOutboxStore(settings, sp.GetRequiredService<ILogger<PostgresOutboxStore>>()), configure);
    }
}
