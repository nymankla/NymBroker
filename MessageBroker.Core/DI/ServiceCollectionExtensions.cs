using MessageBroker.Core.Factory;
using Microsoft.Extensions.DependencyInjection;

namespace MessageBroker.Core.DI;

public static class ServiceCollectionExtensions
{
    public static MessageBrokerBuilder AddMessageBroker(this IServiceCollection services)
        => new(services);
}
