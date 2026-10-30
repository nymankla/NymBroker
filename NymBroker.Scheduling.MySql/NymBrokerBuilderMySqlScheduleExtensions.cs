using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Factory;

namespace NymBroker.Scheduling.MySql;

public static class NymBrokerBuilderMySqlScheduleExtensions
{
    /// <summary>
    /// Named scheduled actions record their runs in a MySQL / MariaDB table shared by every instance that uses it: each
    /// occurrence runs on one instance only, and a run missed while all instances were down can be caught up
    /// (<c>MissedRuns = MissedRunPolicy.RunOnce</c>).
    /// </summary>
    public static NymBrokerBuilder AddMySqlScheduleStore(this NymBrokerBuilder builder, MySqlScheduleStoreSettings? settings = null)
    {
        var s = settings ?? new MySqlScheduleStoreSettings();
        s.Validate();

        builder.Services.RemoveAll<MySqlScheduleStoreSettings>();
        builder.Services.AddSingleton(s);
        builder.Services.RemoveAll<MySqlScheduleStore>();
        builder.Services.AddSingleton(sp => new MySqlScheduleStore(s, sp.GetRequiredService<ILogger<MySqlScheduleStore>>()));
        return builder.AddScheduleStore(sp => sp.GetRequiredService<MySqlScheduleStore>());
    }
}
