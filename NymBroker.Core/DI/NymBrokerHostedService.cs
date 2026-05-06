using NymBroker.Core.Impl;
using Microsoft.Extensions.Hosting;

namespace NymBroker.Core.DI;

internal sealed class NymBrokerHostedService(INymBroker broker) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
        => broker.StartAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken)
        => broker.StopAsync(cancellationToken);
}
