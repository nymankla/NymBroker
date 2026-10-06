using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Factory;

namespace NymBroker.Idempotency.Sqlite;

public static class NymBrokerBuilderSqliteIdempotencyExtensions
{
    /// <summary>
    /// Makes the broker an idempotent receiver backed by a SQLite table (single host). A hosted service deletes expired rows
    /// unless <see cref="SqliteIdempotencySettings.CleanupInterval"/> is zero.
    /// </summary>
    public static NymBrokerBuilder AddSqliteIdempotency(this NymBrokerBuilder builder, SqliteIdempotencySettings? settings = null)
    {
        var s = settings ?? new SqliteIdempotencySettings();
        s.Validate();

        builder.Services.RemoveAll<SqliteIdempotencySettings>();
        builder.Services.RemoveAll<SqliteIdempotencyStore>();
        builder.Services.AddSingleton(s);
        builder.Services.AddSingleton(sp => new SqliteIdempotencyStore(s, sp.GetRequiredService<ILogger<SqliteIdempotencyStore>>()));
        builder.AddIdempotentReceiver(sp => sp.GetRequiredService<SqliteIdempotencyStore>());

        if (s.CleanupInterval > TimeSpan.Zero)
            builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, SqliteIdempotencyCleanupService>());

        return builder;
    }
}
