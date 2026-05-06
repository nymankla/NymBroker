using NymBroker.Core.Factory;
using Microsoft.Extensions.DependencyInjection;

namespace NymBroker.Core.DI;

public static class ServiceCollectionExtensions
{
    public static NymBrokerBuilder AddNymBroker(this IServiceCollection services)
        => new(services);
}
