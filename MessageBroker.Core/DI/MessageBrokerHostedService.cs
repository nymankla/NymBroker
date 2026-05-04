using MessageBroker.Core.Impl;
using Microsoft.Extensions.Hosting;

namespace MessageBroker.Core.DI;

internal sealed class MessageBrokerHostedService(IMessageBroker broker) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
        => broker.StartAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken)
        => broker.StopAsync(cancellationToken);
}
