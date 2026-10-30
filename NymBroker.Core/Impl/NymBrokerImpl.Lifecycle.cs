using System.Runtime.ExceptionServices;
using NymBroker.Core.Endpoint;
using Microsoft.Extensions.Logging;

namespace NymBroker.Core.Impl;

internal sealed partial class NymBrokerImpl
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

            foreach (var outName in _outputTransformers.Keys)
            {
                ValidateTarget(outName, "Output transformer endpoint");
                // The broker would read back the transformed bytes and could not decode them.
                var outEndpoint = _endpoints[outName];
                if (outEndpoint is IEndPointEventDriven && outEndpoint.Mode != EndpointMode.WriteOnly
                    && _globalInputTransformer == null && !_endpointTransformers.ContainsKey(outName))
                    throw new InvalidOperationException(
                        $"Endpoint '{outName}' has an output transformer and the broker also listens on it. Make it WriteOnly, or add an input transformer that reads the transformed format.");
            }

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

    /// <remarks>
    /// Always stops, even when <paramref name="ct"/> is already cancelled: the host passes a cancelled token once its
    /// shutdown timeout has passed, and throwing then would leave every listener running. The lifecycle lock is therefore
    /// taken without the token; a concurrent <see cref="StartAsync"/> holds it only until it has started (the host
    /// cancels the start token on shutdown), and stopping itself takes no token.
    /// A listener that fails to stop does not keep the others running: each failure is logged, every listener is still
    /// stopped and the broker counts as stopped, then the failure is rethrown (several as an <see cref="AggregateException"/>).
    /// </remarks>
    public async Task StopAsync(CancellationToken ct = default)
    {
        await _lifecycleLock.WaitAsync(CancellationToken.None);
        List<Exception>? failures = null;
        try
        {
            if (!_started)
                return;

            await StopScheduledActionsAsync();

            foreach (var (name, endpoint) in _endpoints)
            {
                if (endpoint is not IEndPointEventDriven eventDriven)
                    continue;

                try
                {
                    await eventDriven.StopListeningAsync();
                    _logger.LogInformation("Stopped listening on endpoint '{Name}'", name);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Endpoint '{Name}' failed to stop listening; stopping the other endpoints", name);
                    (failures ??= []).Add(ex);
                }
            }

            _started = false;
            if (failures is null)
                _logger.LogInformation("Broker stopped");
            else
                _logger.LogError("Broker stopped; {Count} endpoint(s) failed to stop listening", failures.Count);
        }
        finally
        {
            _lifecycleLock.Release();
        }

        if (failures is [var single])
            ExceptionDispatchInfo.Throw(single);
        if (failures is not null)
            throw new AggregateException("One or more endpoints failed to stop listening.", failures);
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
