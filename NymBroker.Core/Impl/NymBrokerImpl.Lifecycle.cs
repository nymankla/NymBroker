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

            LogRegisteredEndpoints();
            ValidateConfiguration();

            OpenStartGate();
            await StartUnderLockAsync(ct);
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    /// <summary>From here on, received messages wait for the start to finish (on a new gate after a failed start).</summary>
    private void OpenStartGate()
    {
        if (_startGate.Task.IsFaulted)
            _startGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _startInitiated = true;
        StartHoldingMessages();
    }

    /// <summary>Starts the scheduled actions and the listeners; rolls everything back and rethrows when one fails.</summary>
    private async Task StartUnderLockAsync(CancellationToken ct)
    {
        var listening = new List<(string Name, IEndPointEventDriven Endpoint)>();
        try
        {
            StartScheduledActions(ct);
            await StartListenersAsync(listening, ct);

            _started = true;
            _startGate.TrySetResult();
            _logger.LogInformation("Broker started");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Broker failed to start — rolling back scheduled actions and {Count} started listener(s)", listening.Count);
            await RollBackStartAsync(listening, ex);
            throw;
        }
    }

    /// <summary>
    /// Undoes a failed start, so nothing keeps running behind a broker that reports "not started" and a later
    /// <see cref="StartAsync"/> can try again. Never throws: the start's own exception is the one to report.
    /// </summary>
    private async Task RollBackStartAsync(List<(string Name, IEndPointEventDriven Endpoint)> listening, Exception startFailure)
    {
        await StopScheduledActionsAsync();

        // First release the messages already received (they get Retry), so a listener that waits for its handlers can stop.
        // The faulted gate stays until the next start replaces it, so a message that is just about to wait sees it too.
        _startGate.TrySetException(new BrokerStartFailedException(startFailure));
        _ = _startGate.Task.Exception;   // observed: nobody may be waiting
        _startInitiated = false;

        foreach (var (name, endpoint) in listening)
        {
            try
            {
                await endpoint.StopListeningAsync();
                _logger.LogInformation("Stopped listening on endpoint '{Name}' after the failed start", name);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Endpoint '{Name}' failed to stop listening after the failed start", name);
            }
        }
    }

    private void LogRegisteredEndpoints()
    {
        if (!_logger.IsEnabled(LogLevel.Information)) return;
        foreach (var (name, endpoint) in _endpoints)
            _logger.LogInformation("Endpoint '{Name}' registered ({Mode})", name, endpoint.Mode.ToString());
    }

    /// <summary>Checks that every endpoint the configuration names exists and can be used that way; throws otherwise.</summary>
    private void ValidateConfiguration()
    {
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
            ValidateOutputTransformerNotReadBack(outName);
        }
    }

    // The broker would read back the transformed bytes and could not decode them.
    private void ValidateOutputTransformerNotReadBack(string outName)
    {
        var outEndpoint = _endpoints[outName];
        if (outEndpoint is IEndPointEventDriven && outEndpoint.Mode != EndpointMode.WriteOnly
            && _globalInputTransformer == null && !_endpointTransformers.ContainsKey(outName))
            throw new InvalidOperationException(
                $"Endpoint '{outName}' has an output transformer and the broker also listens on it. Make it WriteOnly, or add an input transformer that reads the transformed format.");
    }

    /// <param name="listening">Receives each endpoint once it listens, for the rollback of a failed start.</param>
    private async Task StartListenersAsync(List<(string Name, IEndPointEventDriven Endpoint)> listening, CancellationToken ct)
    {
        foreach (var (name, endpoint) in _endpoints)
        {
            if (endpoint is IEndPointEventDriven ed && endpoint.Mode != EndpointMode.WriteOnly)
            {
                await ed.StartListeningAsync(
                    (raw, token) => ProcessAsync(raw, name, token),
                    ct);
                listening.Add((name, ed));
                _logger.LogInformation("Started listening on endpoint '{Name}'", name);
            }
            else if (endpoint.Mode == EndpointMode.WriteOnly)
            {
                _logger.LogInformation("Endpoint '{Name}' is write-only — listener not started", name);
            }
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
            // Also for a broker that never started: posting works before the start.
            await StopHoldingMessagesAsync();

            if (!_started)
                return;

            await StopScheduledActionsAsync();
            failures = await StopListenersAsync();

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

        ThrowIfStopFailed(failures);
    }

    /// <summary>One failure is rethrown as it was; several as an <see cref="AggregateException"/>.</summary>
    private static void ThrowIfStopFailed(List<Exception>? failures)
    {
        if (failures is [var single])
            ExceptionDispatchInfo.Throw(single);
        if (failures is not null)
            throw new AggregateException("One or more endpoints failed to stop listening.", failures);
    }

    /// <summary>Stops every listener, also after one fails; returns the failures (logged), or null.</summary>
    private async Task<List<Exception>?> StopListenersAsync()
    {
        List<Exception>? failures = null;
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
        return failures;
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
