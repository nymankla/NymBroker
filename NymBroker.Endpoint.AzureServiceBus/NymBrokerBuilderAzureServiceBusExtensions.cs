using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Factory;

namespace NymBroker.Endpoint.AzureServiceBus;

public static class NymBrokerBuilderAzureServiceBusExtensions
{
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
    /// Call after <c>LoadConfiguration()</c> or <c>ApplyConfiguration()</c>. Config entries use <c>connectionString</c>; to authenticate with a
    /// <c>TokenCredential</c>, register the endpoint in code with <see cref="AddAzureServiceBusEndPoint"/> instead.
    /// </summary>
    public static NymBrokerBuilder WithAzureServiceBus(this NymBrokerBuilder builder)
        => builder.AddConfiguredEndPoints(AzureServiceBusEndPointType.AzureServiceBus,
            ep => builder.AddAzureServiceBusEndPoint(ep.Name, ep.GetSettings<AzureServiceBusSettings>(), ep.Mode));
}
