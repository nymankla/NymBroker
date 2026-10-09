using NymBroker.Core.Endpoint;
using NymBroker.Core.Factory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace NymBroker.Endpoint.RabbitMq;

public static class NymBrokerBuilderRabbitMqExtensions
{
    public static NymBrokerBuilder AddRabbitMqEndPoint(
        this NymBrokerBuilder builder, string name, RabbitMqSettings? settings = null,
        EndpointMode mode = EndpointMode.ReadWrite)
    {
        var s = settings ?? new RabbitMqSettings();
        builder.Services.AddKeyedSingleton<IEndPoint>(name,
            (sp, _) => new RabbitMqEndPoint(name, s, sp.GetRequiredService<ILogger<RabbitMqEndPoint>>(), mode));
        builder.RegisterEndpoint(name);
        return builder;
    }

    /// <summary>
    /// Processes any RabbitMq endpoints from a previously loaded configuration file.
    /// Call after <c>LoadConfiguration()</c> or <c>ApplyConfiguration()</c>:
    /// <code>
    ///   services.AddNymBroker()
    ///       .LoadConfiguration("queuesettings.json")
    ///       .WithRabbitMq()
    ///       .Build();
    /// </code>
    /// </summary>
    public static NymBrokerBuilder WithRabbitMq(this NymBrokerBuilder builder)
        => builder.AddConfiguredEndPoints(EndPointType.RabbitMq,
            ep => builder.AddRabbitMqEndPoint(ep.Name, ep.GetSettings<RabbitMqSettings>(), ep.Mode));
}
