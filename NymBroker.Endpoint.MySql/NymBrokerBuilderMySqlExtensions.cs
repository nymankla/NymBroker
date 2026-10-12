using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Factory;

namespace NymBroker.Endpoint.MySql;

public static class NymBrokerBuilderMySqlExtensions
{
    public static NymBrokerBuilder AddMySqlEndPoint(
        this NymBrokerBuilder builder, string name, MySqlSettings? settings = null,
        EndpointMode mode = EndpointMode.ReadWrite)
    {
        var s = settings ?? new MySqlSettings();
        MySqlQueueSql.QuoteTableName(s.TableName);   // fail at registration, not at first resolve
        builder.Services.AddKeyedSingleton<IEndPoint>(name,
            (sp, _) => new MySqlEndPoint(name, s, sp.GetRequiredService<ILogger<MySqlEndPoint>>(), mode));
        builder.RegisterEndpoint(name);
        return builder;
    }

    /// <summary>
    /// Processes any <c>"Type": "MySql"</c> endpoints from a previously loaded configuration file.
    /// Call after <c>LoadConfiguration()</c> or <c>ApplyConfiguration()</c>.
    /// </summary>
    public static NymBrokerBuilder WithMySql(this NymBrokerBuilder builder)
        => builder.AddConfiguredEndPoints(MySqlEndPointType.MySql,
            ep => builder.AddMySqlEndPoint(ep.Name, ep.GetSettings<MySqlSettings>(), ep.Mode));
}
