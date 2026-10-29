using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Factory;

namespace NymBroker.Scheduling.SqlServer;

public static class NymBrokerBuilderSqlServerScheduleExtensions
{
    /// <summary>
    /// Named scheduled actions record their runs in a SQL Server table shared by every instance that uses it: each
    /// occurrence runs on one instance only, and a run missed while all instances were down can be caught up
    /// (<c>MissedRuns = MissedRunPolicy.RunOnce</c>).
    /// </summary>
    public static NymBrokerBuilder AddSqlServerScheduleStore(this NymBrokerBuilder builder, SqlServerScheduleStoreSettings? settings = null)
    {
        var s = settings ?? new SqlServerScheduleStoreSettings();
        s.Validate();

        builder.Services.RemoveAll<SqlServerScheduleStoreSettings>();
        builder.Services.AddSingleton(s);
        builder.Services.RemoveAll<SqlServerScheduleStore>();
        builder.Services.AddSingleton(sp => new SqlServerScheduleStore(s, sp.GetRequiredService<ILogger<SqlServerScheduleStore>>()));
        return builder.AddScheduleStore(sp => sp.GetRequiredService<SqlServerScheduleStore>());
    }
}
