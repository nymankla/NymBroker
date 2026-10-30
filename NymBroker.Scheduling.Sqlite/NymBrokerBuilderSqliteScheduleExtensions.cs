using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Factory;

namespace NymBroker.Scheduling.Sqlite;

public static class NymBrokerBuilderSqliteScheduleExtensions
{
    /// <summary>
    /// Named scheduled actions record their runs in a SQLite table (single host), so a run missed while the app was down
    /// can be caught up after a restart (<c>MissedRuns = MissedRunPolicy.RunOnce</c>).
    /// </summary>
    public static NymBrokerBuilder AddSqliteScheduleStore(this NymBrokerBuilder builder, SqliteScheduleStoreSettings? settings = null)
    {
        var s = settings ?? new SqliteScheduleStoreSettings();
        s.Validate();

        builder.Services.RemoveAll<SqliteScheduleStoreSettings>();
        builder.Services.AddSingleton(s);
        builder.Services.RemoveAll<SqliteScheduleStore>();
        builder.Services.AddSingleton(sp => new SqliteScheduleStore(s, sp.GetRequiredService<ILogger<SqliteScheduleStore>>()));
        return builder.AddScheduleStore(sp => sp.GetRequiredService<SqliteScheduleStore>());
    }
}
