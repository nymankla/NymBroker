using System.Text.Json;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Factory;
using NymBroker.Core.Factory.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace NymBroker.RabbitMq;

public static class NymBrokerBuilderRabbitMqExtensions
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };

    public static NymBrokerBuilder AddRabbitMqEndPoint(
        this NymBrokerBuilder builder, string name, RabbitMqSettings? settings = null)
    {
        var s = settings ?? new RabbitMqSettings();
        builder.Services.AddKeyedSingleton<IEndPoint>(name,
            (sp, _) => new RabbitMqEndPoint(name, s, sp.GetRequiredService<ILogger<RabbitMqEndPoint>>()));
        builder.RegisterEndpoint(name);
        return builder;
    }

    /// <summary>
    /// Processes any RabbitMq endpoints from a previously loaded configuration file.
    /// Call after <c>LoadConfiguration()</c>:
    /// <code>
    ///   services.AddNymBroker()
    ///       .LoadConfiguration("queuesettings.json")
    ///       .WithRabbitMq()
    ///       .Build();
    /// </code>
    /// </summary>
    public static NymBrokerBuilder WithRabbitMq(this NymBrokerBuilder builder)
    {
        if (builder.LoadedConfiguration is null) return builder;

        foreach (var ep in builder.LoadedConfiguration.Endpoints)
        {
            if (ep.Type == EndPointType.RabbitMq)
                builder.AddRabbitMqEndPoint(ep.Name, ToSettings(ep));
        }

        return builder;
    }

    private static RabbitMqSettings ToSettings(EndPointConfiguration ep)
        => ep.Config.HasValue
            ? JsonSerializer.Deserialize<RabbitMqSettings>(ep.Config.Value.GetRawText(), JsonOptions) ?? new()
            : new();
}
