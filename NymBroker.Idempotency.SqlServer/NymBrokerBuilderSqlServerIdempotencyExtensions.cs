using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Factory;

namespace NymBroker.Idempotency.SqlServer;

public static class NymBrokerBuilderSqlServerIdempotencyExtensions
{
    /// <summary>
    /// Makes the broker an idempotent receiver backed by a SQL Server table, shared by every instance that uses the
    /// same table. Also registers a hosted service that deletes expired rows (unless
    /// <see cref="SqlServerIdempotencySettings.CleanupInterval"/> is zero).
    /// </summary>
    public static NymBrokerBuilder AddSqlServerIdempotency(this NymBrokerBuilder builder, SqlServerIdempotencySettings? settings = null)
    {
        var s = settings ?? new SqlServerIdempotencySettings();
        s.Validate();

        builder.Services.RemoveAll<SqlServerIdempotencySettings>();
        builder.Services.AddSingleton(s);
        return builder.AddIdempotencyStore(
            sp => new SqlServerIdempotencyStore(s, sp.GetRequiredService<ILogger<SqlServerIdempotencyStore>>()), s.CleanupInterval, s.TableName);
    }
}
