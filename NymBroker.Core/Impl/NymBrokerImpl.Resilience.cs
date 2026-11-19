using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Consume;
using NymBroker.Core.Message;
using NymBroker.Core.Resilience;

namespace NymBroker.Core.Impl;

/// <summary>A consumer's timeout and circuit breaker (#134), shared by every message type it consumes.</summary>
internal sealed record ConsumerResilience(string Consumer, TimeSpan? Timeout, CircuitBreaker? Breaker);

/// <summary>Raised by the broker when a consumer passes its <see cref="ConsumerOptions.Timeout"/>; settled with reason <c>ConsumerTimedOut</c>.</summary>
internal sealed class ConsumerTimeoutException(string message, Exception innerException) : TimeoutException(message, innerException);

internal sealed partial class NymBrokerImpl
{
    // Message types whose consumer has a timeout or a circuit breaker. Read for every consumed message.
    private ImmutableDictionary<Type, ConsumerResilience> _consumerResilience = ImmutableDictionary<Type, ConsumerResilience>.Empty;

    /// <summary>The consumer's timeout and breaker for a message type, if it has either.</summary>
    internal ConsumerResilience? ResilienceFor(Type messageType) => _consumerResilience.GetValueOrDefault(messageType);

    private void RegisterConsumerResilience(Type messageType, string serviceKey, ConsumerSettings settings)
    {
        if (settings.Timeout is null && settings.CircuitBreaker is null) return;

        // One breaker per consumer, created per broker, so the message types of one consumer share its state.
        var resilience = _consumerResilience.Values.FirstOrDefault(r => r.Consumer == serviceKey) ?? CreateResilience(serviceKey, settings);
        ImmutableInterlocked.Update(ref _consumerResilience, static (registry, add) => registry.SetItem(add.Type, add.Resilience), (Type: messageType, Resilience: resilience));
    }

    private ConsumerResilience CreateResilience(string consumer, ConsumerSettings settings)
    {
        CircuitBreaker? breaker = null;
        if (settings.CircuitBreaker is { } options)
            breaker = new CircuitBreaker(options, consumer) { StateChanged = state => LogCircuitStateChanged(consumer, state, options.BreakDuration) };
        return new ConsumerResilience(consumer, settings.Timeout, breaker);
    }

    /// <summary>
    /// Calls the consumer, through its circuit breaker and within its timeout when it has them. While the circuit is
    /// open the message is held here (backpressure) rather than failed: a Retry would be redelivered at once and use up
    /// its delivery count. Only the stop token's cancellation ends the wait.
    /// </summary>
    private async Task InvokeConsumerAsync(Type messageType, object message, IMessageContext context, CancellationToken ct)
    {
        if (!_consumerResilience.TryGetValue(messageType, out var resilience))
        {
            await _consumerDispatcher.DispatchAsync(messageType, message, context, ct);
            return;
        }
        if (resilience.Breaker is not null)
            await DispatchThroughBreakerAsync(resilience, messageType, message, context, ct);
        else
            await DispatchWithinTimeoutAsync(resilience, messageType, message, context, ct);
    }

    private async Task DispatchThroughBreakerAsync(ConsumerResilience resilience, Type messageType, object message, IMessageContext context, CancellationToken ct)
    {
        var breaker = resilience.Breaker!;
        var (permit, waited) = await breaker.WaitForPermitAsync(ct);
        if (waited) LogReleasedByCircuit(resilience.Consumer, messageType.Name);
        try
        {
            await DispatchWithinTimeoutAsync(resilience, messageType, message, context, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            breaker.RecordIgnored(permit);   // a shutdown says nothing about the downstream
            throw;
        }
        catch (Exception ex)
        {
            breaker.RecordException(permit, ex);
            throw;
        }
        breaker.RecordSuccess(permit);
    }

    /// <summary>
    /// At the timeout the consumer's token is cancelled and the broker stops waiting at once, so a consumer that ignores
    /// its token cannot hold the listener; its task is observed and what it does later is logged.
    /// </summary>
    private async Task DispatchWithinTimeoutAsync(ConsumerResilience resilience, Type messageType, object message, IMessageContext context, CancellationToken ct)
    {
        if (resilience.Timeout is not { } timeout)
        {
            await _consumerDispatcher.DispatchAsync(messageType, message, context, ct);
            return;
        }

        using var timeoutCts = new CancellationTokenSource(timeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);
        var call = _consumerDispatcher.DispatchAsync(messageType, message, context, linked.Token);
        try
        {
            await call.WaitAsync(timeoutCts.Token);
        }
        catch (OperationCanceledException ex) when (timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            if (!call.IsCompleted) ObserveAbandonedCall(call, resilience.Consumer, messageType);
            ResilienceDiagnostics.ConsumerTimeouts.Add(1, new KeyValuePair<string, object?>("consumer", resilience.Consumer));
            throw new ConsumerTimeoutException($"Consumer '{resilience.Consumer}' did not finish within its timeout of {timeout}.", ex);
        }
    }

    private void ObserveAbandonedCall(Task call, string consumer, Type messageType)
        => _ = call.ContinueWith(t => LogAbandonedCallEnded(t, consumer, messageType),
            CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

    private void LogAbandonedCallEnded(Task call, string consumer, Type messageType)
    {
        if (call.IsFaulted)
            _logger.LogWarning(call.Exception.GetBaseException(),
                "Consumer '{Consumer}' failed after its timeout had settled message type {MessageType}", consumer, messageType.Name);
        else
            _logger.LogDebug("Consumer '{Consumer}' ended {Outcome} after its timeout had settled message type {MessageType}",
                consumer, call.IsCanceled ? "cancelled" : "successfully", messageType.Name);
    }

    private void LogCircuitStateChanged(string consumer, CircuitState state, TimeSpan breakDuration)
    {
        switch (state)
        {
            case CircuitState.Open:
                _logger.LogWarning("Circuit of consumer '{Consumer}' opened: its messages are held for {BreakDuration} before one is tried again", consumer, breakDuration);
                break;
            case CircuitState.HalfOpen:
                _logger.LogInformation("Circuit of consumer '{Consumer}' is half-open: trying one message", consumer);
                break;
            default:
                _logger.LogInformation("Circuit of consumer '{Consumer}' closed: its messages flow again", consumer);
                break;
        }
    }

    [LoggerMessage(LogLevel.Debug, "Message type {MessageType} for consumer '{Consumer}' was held by its open circuit and is now let through")]
    private partial void LogReleasedByCircuit(string consumer, string messageType);
}
