using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Factory;
using NymBroker.Core.Factory.Configuration;

namespace NymBroker.Sql;

public static class NymBrokerBuilderSqliteExtensions
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };

    public static NymBrokerBuilder AddSqliteEndPoint(
        this NymBrokerBuilder builder, string name, SqliteSettings? settings = null)
    {
        var s = settings ?? new SqliteSettings();
        builder.Services.AddKeyedSingleton<IEndPoint>(name,
            (sp, _) => new SqliteEndPoint(name, s, sp.GetRequiredService<ILogger<SqliteEndPoint>>()));
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
                builder.AddSqliteEndPoint(ep.Name, ToSettings(ep));
        }

        return builder;
    }

    private static SqliteSettings ToSettings(EndPointConfiguration ep)
        => ep.Config.HasValue
            ? JsonSerializer.Deserialize<SqliteSettings>(ep.Config.Value.GetRawText(), JsonOptions) ?? new()
            : new();
}
