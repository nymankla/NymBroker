using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Factory;
using NymBroker.Core.Factory.Configuration;

namespace NymBroker.AzureServiceBus;

public static class NymBrokerBuilderAzureServiceBusExtensions
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };

    public static NymBrokerBuilder AddAzureServiceBusEndPoint(
        this NymBrokerBuilder builder, string name, AzureServiceBusSettings settings,
        EndpointMode mode = EndpointMode.ReadWrite)
    {
        settings.Validate(name);   // fail at registration, not at first resolve
        builder.Services.AddKeyedSingleton<IEndPoint>(name,
            (sp, _) => new AzureServiceBusEndPoint(name, settings, sp.GetRequiredService<ILogger<AzureServiceBusEndPoint>>(), mode));
        builder.RegisterEndpoint(name);
        return builder;
    }

    /// <summary>
    /// Processes any <c>"Type": "AzureServiceBus"</c> endpoints from a previously loaded configuration file.
    /// Call after <c>LoadConfiguration()</c>. Config entries use <c>connectionString</c>; to authenticate with a
    /// <c>TokenCredential</c>, register the endpoint in code with <see cref="AddAzureServiceBusEndPoint"/> instead.
    /// </summary>
    public static NymBrokerBuilder WithAzureServiceBus(this NymBrokerBuilder builder)
    {
        if (builder.LoadedConfiguration is null) return builder;

        foreach (var ep in builder.LoadedConfiguration.Endpoints)
        {
            if (ep.IsType(AzureServiceBusEndPointType.AzureServiceBus))
                builder.AddAzureServiceBusEndPoint(ep.Name, ToSettings(ep), ep.Mode);
        }

        return builder;
    }

    private static AzureServiceBusSettings ToSettings(EndPointConfiguration ep)
        => ep.Config.HasValue
            ? JsonSerializer.Deserialize<AzureServiceBusSettings>(ep.Config.Value.GetRawText(), JsonOptions) ?? new()
            : new();
}
