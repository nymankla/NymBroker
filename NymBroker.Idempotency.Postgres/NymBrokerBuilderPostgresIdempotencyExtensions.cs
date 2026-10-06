using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
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
        builder.Services.RemoveAll<PostgresIdempotencyStore>();
        builder.Services.AddSingleton(s);
        builder.Services.AddSingleton(sp => new PostgresIdempotencyStore(s, sp.GetRequiredService<ILogger<PostgresIdempotencyStore>>()));
        builder.AddIdempotentReceiver(sp => sp.GetRequiredService<PostgresIdempotencyStore>());

        if (s.CleanupInterval > TimeSpan.Zero)
            builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, PostgresIdempotencyCleanupService>());

        return builder;
    }
}
