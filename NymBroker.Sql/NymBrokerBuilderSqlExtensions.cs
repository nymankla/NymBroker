using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Factory;
using NymBroker.Core.Factory.Configuration;

namespace NymBroker.Sql;

public static class NymBrokerBuilderSqlExtensions
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };

    public static NymBrokerBuilder AddSqlEndPoint(
        this NymBrokerBuilder builder, string name, SqlSettings? settings = null)
    {
        var s = settings ?? new SqlSettings();
        builder.Services.AddKeyedSingleton<IEndPoint>(name,
            (sp, _) => new SqlEndPoint(name, s, sp.GetRequiredService<ILogger<SqlEndPoint>>()));
        builder.RegisterEndpoint(name);
        return builder;
    }

    /// <summary>
    /// Processes any Sql endpoints from a previously loaded configuration file.
    /// Call after <c>LoadConfiguration()</c>.
    /// </summary>
    public static NymBrokerBuilder WithSql(this NymBrokerBuilder builder)
    {
        if (builder.LoadedConfiguration is null) return builder;

        foreach (var ep in builder.LoadedConfiguration.Endpoints)
        {
            if (ep.Type == EndPointType.Sql)
                builder.AddSqlEndPoint(ep.Name, ToSettings(ep));
        }

        return builder;
    }

    private static SqlSettings ToSettings(EndPointConfiguration ep)
        => ep.Config.HasValue
            ? JsonSerializer.Deserialize<SqlSettings>(ep.Config.Value.GetRawText(), JsonOptions) ?? new()
            : new();
}
