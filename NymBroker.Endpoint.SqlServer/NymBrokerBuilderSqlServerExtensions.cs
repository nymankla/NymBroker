using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Factory;

namespace NymBroker.Endpoint.SqlServer;

public static class NymBrokerBuilderSqlServerExtensions
{
    public static NymBrokerBuilder AddSqlServerEndPoint(
        this NymBrokerBuilder builder, string name, SqlServerSettings? settings = null,
        EndpointMode mode = EndpointMode.ReadWrite)
    {
        var s = settings ?? new SqlServerSettings();
        builder.Services.AddKeyedSingleton<IEndPoint>(name,
            (sp, _) => new SqlServerEndPoint(name, s, sp.GetRequiredService<ILogger<SqlServerEndPoint>>(), mode));
        builder.RegisterEndpoint(name);
        return builder;
    }

    /// <summary>
    /// Processes any <c>"Type": "SqlServer"</c> endpoints from a previously loaded configuration file.
    /// Call after <c>LoadConfiguration()</c> or <c>ApplyConfiguration()</c>.
    /// </summary>
    public static NymBrokerBuilder WithSqlServer(this NymBrokerBuilder builder)
        => builder.AddConfiguredEndPoints(SqlServerEndPointType.SqlServer,
            ep => builder.AddSqlServerEndPoint(ep.Name, ep.GetSettings<SqlServerSettings>(), ep.Mode));
}
