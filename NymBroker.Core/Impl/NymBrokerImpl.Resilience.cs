using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Consume;
using NymBroker.Core.Message;
using NymBroker.Core.Resilience;

namespace NymBroker.Core.Impl;

/// <summary>A consumer's timeout and circuit breaker (#134) and its rate and concurrency limit (#136), shared by every message type it consumes.</summary>
internal sealed record ConsumerResilience(string Consumer, TimeSpan? Timeout, CircuitBreaker? Breaker, ThrottleGate? Throttle = null);

/// <summary>Raised by the broker when a consumer passes its <see cref="ConsumerOptions.Timeout"/>; settled with reason <c>ConsumerTimedOut</c>.</summary>
internal sealed class ConsumerTimeoutException(string message, Exception innerException) : TimeoutException(message, innerException);

internal sealed partial class NymBrokerImpl
{
    // Message types whose consumer has a timeout, a circuit breaker or a limit. Read for every consumed message.
    private ImmutableDictionary<Type, ConsumerResilience> _consumerResilience = ImmutableDictionary<Type, ConsumerResilience>.Empty;

    /// <summary>The consumer's timeout, breaker and limits for a message type, if it has any.</summary>
    internal ConsumerResilience? ResilienceFor(Type messageType) => _consumerResilience.GetValueOrDefault(messageType);

    private void RegisterConsumerResilience(Type messageType, string serviceKey, ConsumerSettings settings)
    {
        if (!settings.HasResilience) return;

        // One breaker per consumer, created per broker, so the message types of one consumer share its state.
        var resilience = _consumerResilience.Values.FirstOrDefault(r => r.Consumer == serviceKey) ?? CreateResilience(serviceKey, settings);
        ImmutableInterlocked.Update(ref _consumerResilience, static (registry, add) => registry.SetItem(add.Type, add.Resilience), (Type: messageType, Resilience: resilience));
    }

    private ConsumerResilience CreateResilience(string consumer, ConsumerSettings settings)
    {
        CircuitBreaker? breaker = null;
        if (settings.CircuitBreaker is { } options)
            breaker = new CircuitBreaker(options, consumer) { StateChanged = state => LogCircuitStateChanged(consumer, state, options.BreakDuration) };
        return new ConsumerResilience(consumer, settings.Timeout, breaker,
            ThrottleGate.For("consumer", consumer, settings.RateLimit, settings.ConcurrencyLimit));
    }

    /// <summary>
    /// Calls the consumer through its circuit breaker, then its limits, then within its timeout, as far as it has them.
    /// While the circuit is open or a limit is reached the message waits here (backpressure) rather than failing: a Retry
    /// would be redelivered at once and use up its delivery count. Only the stop token's cancellation ends a wait; the
    /// timeout counts only the call itself.
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
            await DispatchThrottledAsync(resilience, messageType, message, context, ct);
    }

    private async Task DispatchThroughBreakerAsync(ConsumerResilience resilience, Type messageType, object message, IMessageContext context, CancellationToken ct)
    {
        var breaker = resilience.Breaker!;
        var (permit, waited) = await breaker.WaitForPermitAsync(ct);
        if (waited) LogReleasedByCircuit(resilience.Consumer, messageType.Name);
        try
        {
            await DispatchThrottledAsync(resilience, messageType, message, context, ct);
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
    /// Waits for a concurrency slot and a rate permit, then calls the consumer. The slot is freed when the broker is done
    /// with the call, so a timed-out call that keeps running in the background no longer counts.
    /// </summary>
    private async Task DispatchThrottledAsync(ConsumerResilience resilience, Type messageType, object message, IMessageContext context, CancellationToken ct)
    {
        if (resilience.Throttle is not { } throttle)
        {
            await DispatchWithinTimeoutAsync(resilience, messageType, message, context, ct);
            return;
        }
        using var lease = await throttle.EnterAsync(1, ct);
        await DispatchWithinTimeoutAsync(resilience, messageType, message, context, ct);
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
