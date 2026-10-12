using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Factory;

namespace NymBroker.Endpoint.Http;

public static class NymBrokerBuilderHttpExtensions
{
    /// <summary>
    /// Registers a send-only HTTP endpoint and a named <c>HttpClient</c> (<c>"NymBroker.Http." + name</c>) for it with
    /// <c>IHttpClientFactory</c>, with <see cref="HttpSettings.Timeout"/> as its timeout. Use
    /// <paramref name="configureClient"/> to add your own handlers, e.g. authentication or a proxy.
    /// </summary>
    public static NymBrokerBuilder AddHttpEndPoint(
        this NymBrokerBuilder builder, string name, HttpSettings settings, Action<IHttpClientBuilder>? configureClient = null)
    {
        settings.Validate(name);   // fail at registration, not at first resolve
        var clientName = HttpClientName(name);
        var client = builder.Services.AddHttpClient(clientName, c => c.Timeout = settings.Timeout);
        configureClient?.Invoke(client);

        builder.Services.AddKeyedSingleton<IEndPoint>(name, (sp, _) => new HttpEndPoint(
            name, settings,
            sp.GetRequiredService<IHttpClientFactory>().CreateClient(clientName),
            sp.GetRequiredService<ILogger<HttpEndPoint>>()));
        builder.RegisterEndpoint(name);
        return builder;
    }

    /// <summary>
    /// Processes any <c>"Type": "Http"</c> endpoints from a previously loaded configuration file.
    /// Call after <c>LoadConfiguration()</c> or <c>ApplyConfiguration()</c>.
    /// </summary>
    public static NymBrokerBuilder WithHttp(this NymBrokerBuilder builder)
        => builder.AddConfiguredEndPoints(HttpEndPointType.Http,
            ep => builder.AddHttpEndPoint(ep.Name, ep.GetSettings<HttpSettings>()));

    internal static string HttpClientName(string endpointName) => $"NymBroker.Http.{endpointName}";
}
