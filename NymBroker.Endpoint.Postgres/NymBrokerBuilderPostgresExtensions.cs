using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Factory;

namespace NymBroker.Endpoint.Postgres;

public static class NymBrokerBuilderPostgresExtensions
{
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
        => builder.AddConfiguredEndPoints(EndPointType.Postgres,
            ep => builder.AddPostgresEndPoint(ep.Name, ep.GetSettings<PostgresSettings>(), ep.Mode));
}
