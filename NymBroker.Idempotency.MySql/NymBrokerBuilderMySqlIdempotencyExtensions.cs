using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Factory;

namespace NymBroker.Idempotency.MySql;

public static class NymBrokerBuilderMySqlIdempotencyExtensions
{
    /// <summary>
    /// Makes the broker an idempotent receiver backed by a MySQL table, shared by every instance that uses the same
    /// table. Also registers a hosted service that deletes expired rows (unless
    /// <see cref="MySqlIdempotencySettings.CleanupInterval"/> is zero).
    /// </summary>
    public static NymBrokerBuilder AddMySqlIdempotency(this NymBrokerBuilder builder, MySqlIdempotencySettings? settings = null)
    {
        var s = settings ?? new MySqlIdempotencySettings();
        s.Validate();

        builder.Services.RemoveAll<MySqlIdempotencySettings>();
        builder.Services.AddSingleton(s);
        return builder.AddIdempotencyStore(
            sp => new MySqlIdempotencyStore(s, sp.GetRequiredService<ILogger<MySqlIdempotencyStore>>()), s.CleanupInterval, s.TableName);
    }
}
