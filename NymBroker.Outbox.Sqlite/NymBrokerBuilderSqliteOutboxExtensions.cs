using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Factory;
using NymBroker.Core.Outbox;

namespace NymBroker.Outbox.Sqlite;

public static class NymBrokerBuilderSqliteOutboxExtensions
{
    /// <summary>
    /// Adds a transactional outbox in a SQLite database file (one host): inject <see cref="IOutbox"/> and post in your
    /// <c>SqliteTransaction</c>; a hosted dispatcher sends the messages once the transaction commits.
    /// </summary>
    /// <param name="configure">Dispatcher options: poll interval, batch size, lease, retries.</param>
    public static NymBrokerBuilder AddSqliteOutbox(this NymBrokerBuilder builder, SqliteOutboxSettings settings, Action<OutboxOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();

        builder.Services.RemoveAll<SqliteOutboxSettings>();
        builder.Services.AddSingleton(settings);
        return builder.AddOutboxStore(sp => new SqliteOutboxStore(settings, sp.GetRequiredService<ILogger<SqliteOutboxStore>>()), configure);
    }
}
