using NymBroker.Core.Endpoint;
using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Impl;

public sealed partial class NymBrokerImpl
{
    public async Task StartAsync(CancellationToken ct = default)
    {
        await _lifecycleLock.WaitAsync(ct);
        try
        {
            if (_started)
                return;

            _startInitiated = true;

            // Log all registered endpoints and validate mode constraints.
            if (_logger.IsEnabled(LogLevel.Information))
                foreach (var (name, endpoint) in _endpoints)
                    _logger.LogInformation("Endpoint '{Name}' registered ({Mode})", name, endpoint.Mode.ToString());

            foreach (var route in _routes)
                ValidateTarget(route.DestinationEndpoint, "Route target");

            foreach (var topic in _topics)
                foreach (var epName in topic.SubscriberEndpoints)
                    ValidateTarget(epName, $"Topic '{topic.TopicName}' target");

            if (_deadLetterEndpoint != null)
                ValidateTarget(_deadLetterEndpoint, "Dead letter endpoint");

            foreach (var tapName in _wireTapEndpoints)
                ValidateTarget(tapName, "Wire tap endpoint");

            try
            {
                StartScheduledActions(ct);

                foreach (var (name, endpoint) in _endpoints)
                {
                    if (endpoint is IEndPointEventDriven ed && endpoint.Mode != EndpointMode.WriteOnly)
                    {
                        await ed.StartListeningAsync(
                            (raw, token) => ProcessAsync(raw, name, token),
                            ct);
                        _logger.LogInformation("Started listening on endpoint '{Name}'", name);
                    }
                    else if (endpoint.Mode == EndpointMode.WriteOnly)
                    {
                        _logger.LogInformation("Endpoint '{Name}' is write-only — listener not started", name);
                    }
                }

                _started = true;
                _startGate.TrySetResult();
                _logger.LogInformation("Broker started");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Broker failed to start — rolling back scheduled actions");
                await StopScheduledActionsAsync();

                throw;
            }
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    public async Task StopAsync(CancellationToken ct = default)
    {
        await _lifecycleLock.WaitAsync(ct);
        try
        {
            if (!_started)
                return;

            await StopScheduledActionsAsync();

            foreach (var kvp in _endpoints.Where(kvp => kvp.Value is IEndPointEventDriven))
            {
                await ((IEndPointEventDriven)kvp.Value).StopListeningAsync();
                if (_logger.IsEnabled(LogLevel.Information))
                    _logger.LogInformation("Stopped listening on endpoint '{Name}'", kvp.Key);
            }

            _started = false;
            _logger.LogInformation("Broker stopped");
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    // A missing target would only show up when the first message is lost, so fail at startup instead.
    private void ValidateTarget(string name, string what)
    {
        if (!_endpoints.TryGetValue(name, out var endpoint))
            throw new InvalidOperationException($"{what} '{name}' is not a registered endpoint.");
        if (endpoint.Mode == EndpointMode.ReadOnly)
            throw new InvalidOperationException(
                $"{what} '{name}' is read-only and cannot receive messages.");
    }
}
