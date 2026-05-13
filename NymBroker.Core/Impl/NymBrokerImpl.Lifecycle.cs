using System.Collections.Immutable;
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
            {
                if (_endpoints.TryGetValue(route.DestinationEndpoint, out var dest) && dest.Mode == EndpointMode.ReadOnly)
                    throw new InvalidOperationException(
                        $"Route targets read-only endpoint '{route.DestinationEndpoint}'. Read-only endpoints cannot receive posted messages.");
            }

            foreach (var topic in _topics)
            {
                foreach (var epName in topic.SubscriberEndpoints)
                {
                    if (_endpoints.TryGetValue(epName, out var dest) && dest.Mode == EndpointMode.ReadOnly)
                        throw new InvalidOperationException(
                            $"Topic '{topic.TopicName}' targets read-only endpoint '{epName}'. Read-only endpoints cannot receive posted messages.");
                }
            }

            if (_deadLetterEndpoint != null
                && _endpoints.TryGetValue(_deadLetterEndpoint, out var dlqDest)
                && dlqDest.Mode == EndpointMode.ReadOnly)
                throw new InvalidOperationException(
                    $"Dead letter endpoint '{_deadLetterEndpoint}' is read-only and cannot receive messages.");

            foreach (var tapName in _wireTapEndpoints)
                if (_endpoints.TryGetValue(tapName, out var tapDest) && tapDest.Mode == EndpointMode.ReadOnly)
                    throw new InvalidOperationException(
                        $"Wire tap endpoint '{tapName}' is read-only and cannot receive messages.");

            var startedScheduledActions = ImmutableList<ScheduledActionHandle>.Empty;
            try
            {
                foreach (var scheduledAction in _scheduledActions)
                    startedScheduledActions = startedScheduledActions.Add(await scheduledAction(ct));

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

                _activeScheduledActions = startedScheduledActions;
                _started = true;
                _startGate.TrySetResult();
                _logger.LogInformation("Broker started");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Broker failed to start — rolling back scheduled actions");
                foreach (var scheduledAction in startedScheduledActions)
                    await scheduledAction.DisposeAsync();

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

            foreach (var scheduledAction in _activeScheduledActions)
                await scheduledAction.DisposeAsync();

            _activeScheduledActions = ImmutableList<ScheduledActionHandle>.Empty;

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
}
