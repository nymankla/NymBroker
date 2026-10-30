using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Factory;

namespace NymBroker.Scheduling.Postgres;

public static class NymBrokerBuilderPostgresScheduleExtensions
{
    /// <summary>
    /// Named scheduled actions record their runs in a PostgreSQL table shared by every instance that uses it: each
    /// occurrence runs on one instance only, and a run missed while all instances were down can be caught up
    /// (<c>MissedRuns = MissedRunPolicy.RunOnce</c>).
    /// </summary>
    public static NymBrokerBuilder AddPostgresScheduleStore(this NymBrokerBuilder builder, PostgresScheduleStoreSettings? settings = null)
    {
        var s = settings ?? new PostgresScheduleStoreSettings();
        s.Validate();

        builder.Services.RemoveAll<PostgresScheduleStoreSettings>();
        builder.Services.AddSingleton(s);
        builder.Services.RemoveAll<PostgresScheduleStore>();
        builder.Services.AddSingleton(sp => new PostgresScheduleStore(s, sp.GetRequiredService<ILogger<PostgresScheduleStore>>()));
        return builder.AddScheduleStore(sp => sp.GetRequiredService<PostgresScheduleStore>());
    }
}
