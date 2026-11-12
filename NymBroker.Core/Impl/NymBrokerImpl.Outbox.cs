using NymBroker.Core.Endpoint;
using NymBroker.Core.Outbox;

namespace NymBroker.Core.Impl;

// The broker's side of the transactional outbox: the envelope is made when the message is written, sent when it is due.
internal sealed partial class NymBrokerImpl
{
    /// <summary>The envelope for an outbox post, made as <see cref="PostAsync{T}"/> or <see cref="PostDeferredAsync{T}"/> would.</summary>
    internal OutboxMessage CreateOutboxMessage<T>(string endpointName, T message, DateTimeOffset? deliverAt) where T : class
    {
        ResolvePostTarget(endpointName);   // unknown / read-only throw now, not when the dispatcher sends
        var context = NewContext(message, endpointName);
        if (deliverAt is { } due && due.UtcDateTime > context.Created) context.Created = due.UtcDateTime;

        return new OutboxMessage(endpointName, _serializer.SerializeRegistered(context), deliverAt);
    }

    internal OutboxMessage CreateRawOutboxMessage(string endpointName, ReadOnlyMemory<byte> message)
    {
        ResolvePostTarget(endpointName);
        return new OutboxMessage(endpointName, message.ToArray());
    }

    internal bool CanSendTo(string endpointName)
        => _endpoints.TryGetValue(endpointName, out var endpoint) && endpoint.Mode != EndpointMode.ReadOnly;

    /// <summary>
    /// Sends a claimed outbox message through the output transformer. One not yet due on the broker's clock (the database
    /// clock released it) goes the deferred way, so it is never delivered early.
    /// </summary>
    internal Task SendFromOutboxAsync(ClaimedOutboxMessage message, CancellationToken ct)
        => message.DeliverAt is { } due && due > DateTimeOffset.UtcNow
            ? SendDeferredAsync(message.EndpointName, message.Body, due, ct)
            : SendAsync(message.EndpointName, ResolvePostTarget(message.EndpointName), message.Body, ct);
}
