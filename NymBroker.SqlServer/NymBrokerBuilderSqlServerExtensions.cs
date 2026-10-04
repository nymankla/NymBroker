using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Factory;
using NymBroker.Core.Factory.Configuration;

namespace NymBroker.SqlServer;

public static class NymBrokerBuilderSqlServerExtensions
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };

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
    /// Call after <c>LoadConfiguration()</c>.
    /// </summary>
    public static NymBrokerBuilder WithSqlServer(this NymBrokerBuilder builder)
    {
        if (builder.LoadedConfiguration is null) return builder;

        foreach (var ep in builder.LoadedConfiguration.Endpoints)
        {
            if (ep.IsType(SqlServerEndPointType.SqlServer))
                builder.AddSqlServerEndPoint(ep.Name, ToSettings(ep), ep.Mode);
        }

        return builder;
    }

    private static SqlServerSettings ToSettings(EndPointConfiguration ep)
        => ep.Config.HasValue
            ? JsonSerializer.Deserialize<SqlServerSettings>(ep.Config.Value.GetRawText(), JsonOptions) ?? new()
            : new();
}
