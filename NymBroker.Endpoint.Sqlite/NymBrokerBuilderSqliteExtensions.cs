using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Factory;

namespace NymBroker.Endpoint.Sqlite;

public static class NymBrokerBuilderSqliteExtensions
{
    public static NymBrokerBuilder AddSqliteEndPoint(
        this NymBrokerBuilder builder, string name, SqliteSettings? settings = null,
        EndpointMode mode = EndpointMode.ReadWrite)
    {
        var s = settings ?? new SqliteSettings();
        builder.Services.AddKeyedSingleton<IEndPoint>(name,
            (sp, _) => new SqliteEndPoint(name, s, sp.GetRequiredService<ILogger<SqliteEndPoint>>(), mode));
        builder.RegisterEndpoint(name);
        return builder;
    }

    /// <summary>
    /// Processes any Sql endpoints from a previously loaded configuration file.
    /// Call after <c>LoadConfiguration()</c> or <c>ApplyConfiguration()</c>.
    /// </summary>
    public static NymBrokerBuilder WithSql(this NymBrokerBuilder builder)
        => builder.AddConfiguredEndPoints(EndPointType.Sql,
            ep => builder.AddSqliteEndPoint(ep.Name, ep.GetSettings<SqliteSettings>(), ep.Mode));
}
