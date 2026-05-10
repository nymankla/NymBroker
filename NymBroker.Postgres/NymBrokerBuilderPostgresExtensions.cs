using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Factory;
using NymBroker.Core.Factory.Configuration;

namespace NymBroker.Postgres;

public static class NymBrokerBuilderPostgresExtensions
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };

    public static NymBrokerBuilder AddPostgresEndPoint(
        this NymBrokerBuilder builder, string name, PostgresSettings? settings = null,
        EndpointMode mode = EndpointMode.ReadWrite)
    {
        var s = settings ?? new PostgresSettings();
        builder.Services.AddKeyedSingleton<IEndPoint>(name,
            (sp, _) => new PostgresEndPoint(name, s, sp.GetRequiredService<ILogger<PostgresEndPoint>>(), mode));
        builder.RegisterEndpoint(name);
        return builder;
    }

    public static NymBrokerBuilder WithPostgres(this NymBrokerBuilder builder)
    {
        if (builder.LoadedConfiguration is null) return builder;

        foreach (var ep in builder.LoadedConfiguration.Endpoints)
        {
            if (ep.Type == EndPointType.Postgres)
                builder.AddPostgresEndPoint(ep.Name, ToSettings(ep), ep.Mode);
        }

        return builder;
    }

    private static PostgresSettings ToSettings(EndPointConfiguration ep)
        => ep.Config.HasValue
            ? JsonSerializer.Deserialize<PostgresSettings>(ep.Config.Value.GetRawText(), JsonOptions) ?? new()
            : new();
}
