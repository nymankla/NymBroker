using System.Collections.Frozen;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Resilience;

namespace NymBroker.Core.Impl;

/// <summary>
/// Send limits per destination endpoint (#136, <c>NymBrokerBuilder.LimitSendsTo</c>). Every write to an endpoint goes
/// through these helpers, so the limit covers posts, routes, topics, wire taps, dead letters, deferred, held and outbox
/// sends. Waiting is backpressure: a route forward holds its source message, and <c>PostAsync</c> waits (on its token).
/// </summary>
internal sealed partial class NymBrokerImpl
{
    private FrozenDictionary<string, ThrottleGate> _sendThrottles = EmptyRegistry<ThrottleGate>();

    /// <summary>Limits what the broker sends to <paramref name="endpointName"/> (configuration time).</summary>
    internal void LimitSends(string endpointName, RateLimitOptions? rateLimit, int? concurrencyLimit)
    {
        if (ThrottleGate.For("endpoint", endpointName, rateLimit, concurrencyLimit) is { } gate)
            _sendThrottles = WithItem(_sendThrottles, endpointName, gate);
    }

    /// <summary>The send limit of an endpoint, if it has one.</summary>
    internal ThrottleGate? SendThrottleFor(string endpointName) => _sendThrottles.GetValueOrDefault(endpointName);

    private Task WriteAsync(string name, IEndPoint endpoint, byte[] message, CancellationToken ct)
        => _sendThrottles.TryGetValue(name, out var gate) ? WriteThrottledAsync(gate, endpoint, message, ct) : endpoint.PostAsync(message, ct);

    private static async Task WriteThrottledAsync(ThrottleGate gate, IEndPoint endpoint, byte[] message, CancellationToken ct)
    {
        using var lease = await gate.EnterAsync(1, ct);
        await endpoint.PostAsync(message, ct);
    }

    /// <summary>A batch takes one concurrency slot and one rate permit per message.</summary>
    private Task WriteBatchAsync(string name, IEndPoint endpoint, IReadOnlyList<byte[]> messages, CancellationToken ct)
        => _sendThrottles.TryGetValue(name, out var gate) ? WriteBatchThrottledAsync(gate, endpoint, messages, ct) : endpoint.PostBatchAsync(messages, ct);

    private static async Task WriteBatchThrottledAsync(ThrottleGate gate, IEndPoint endpoint, IReadOnlyList<byte[]> messages, CancellationToken ct)
    {
        using var lease = await gate.EnterAsync(messages.Count, ct);
        await endpoint.PostBatchAsync(messages, ct);
    }

    private async Task WriteDeferredAsync(string name, IDeferredEndPoint endpoint, byte[] message, DateTimeOffset deliverAt, CancellationToken ct)
    {
        if (!_sendThrottles.TryGetValue(name, out var gate))
        {
            await endpoint.PostDeferredAsync(message, deliverAt, ct);
            return;
        }
        using var lease = await gate.EnterAsync(1, ct);
        await endpoint.PostDeferredAsync(message, deliverAt, ct);
    }
}
