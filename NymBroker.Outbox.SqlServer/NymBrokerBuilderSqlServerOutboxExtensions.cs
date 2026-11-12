using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Factory;
using NymBroker.Core.Outbox;

namespace NymBroker.Outbox.SqlServer;

public static class NymBrokerBuilderSqlServerOutboxExtensions
{
    /// <summary>
    /// Adds a transactional outbox in a SQL Server table: inject <see cref="IOutbox"/> and post in your
    /// <c>SqlTransaction</c>; a hosted dispatcher sends the messages once the transaction commits.
    /// </summary>
    /// <param name="configure">Dispatcher options: poll interval, batch size, lease, retries.</param>
    public static NymBrokerBuilder AddSqlServerOutbox(this NymBrokerBuilder builder, SqlServerOutboxSettings? settings = null, Action<OutboxOptions>? configure = null)
    {
        var s = settings ?? new SqlServerOutboxSettings();
        s.Validate();

        builder.Services.RemoveAll<SqlServerOutboxSettings>();
        builder.Services.AddSingleton(s);
        return builder.AddOutboxStore(sp => new SqlServerOutboxStore(s, sp.GetRequiredService<ILogger<SqlServerOutboxStore>>()), configure);
    }
}
