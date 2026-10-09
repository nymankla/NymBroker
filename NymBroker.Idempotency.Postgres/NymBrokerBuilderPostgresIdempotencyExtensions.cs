using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Factory;

namespace NymBroker.Idempotency.Postgres;

public static class NymBrokerBuilderPostgresIdempotencyExtensions
{
    /// <summary>
    /// Makes the broker an idempotent receiver backed by a PostgreSQL table. A hosted service deletes expired rows
    /// unless <see cref="PostgresIdempotencySettings.CleanupInterval"/> is zero.
    /// </summary>
    public static NymBrokerBuilder AddPostgresIdempotency(this NymBrokerBuilder builder, PostgresIdempotencySettings? settings = null)
    {
        var s = settings ?? new PostgresIdempotencySettings();
        s.Validate();

        builder.Services.RemoveAll<PostgresIdempotencySettings>();
        builder.Services.AddSingleton(s);
        return builder.AddIdempotencyStore(
            sp => new PostgresIdempotencyStore(s, sp.GetRequiredService<ILogger<PostgresIdempotencyStore>>()), s.CleanupInterval, s.TableName);
    }
}
