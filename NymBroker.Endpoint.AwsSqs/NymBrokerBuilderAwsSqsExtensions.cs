using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Factory;

namespace NymBroker.Endpoint.AwsSqs;

public static class NymBrokerBuilderAwsSqsExtensions
{
    public static NymBrokerBuilder AddAwsSqsEndPoint(
        this NymBrokerBuilder builder, string name, AwsSqsSettings settings,
        EndpointMode mode = EndpointMode.ReadWrite)
    {
        settings.Validate(name);   // fail at registration, not at first resolve
        builder.Services.AddKeyedSingleton<IEndPoint>(name,
            (sp, _) => new AwsSqsEndPoint(name, settings, sp.GetRequiredService<ILogger<AwsSqsEndPoint>>(), mode));
        builder.RegisterEndpoint(name);
        return builder;
    }

    /// <summary>
    /// Processes any <c>"Type": "AwsSqs"</c> endpoints from a previously loaded configuration file.
    /// Call after <c>LoadConfiguration()</c> or <c>ApplyConfiguration()</c>. Credentials come from the SDK's default chain;
    /// to pass explicit <c>AWSCredentials</c>, register the endpoint in code with <see cref="AddAwsSqsEndPoint"/> instead.
    /// </summary>
    public static NymBrokerBuilder WithAwsSqs(this NymBrokerBuilder builder)
        => builder.AddConfiguredEndPoints(AwsSqsEndPointType.AwsSqs,
            ep => builder.AddAwsSqsEndPoint(ep.Name, ep.GetSettings<AwsSqsSettings>(), ep.Mode));
}
