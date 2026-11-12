namespace NymBroker.Core.Outbox;

/// <summary>A message to write to the outbox: the bytes to send and where to.</summary>
public sealed class OutboxMessage
{
    public OutboxMessage(string endpointName, byte[] body, DateTimeOffset? deliverAt = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpointName);
        ArgumentNullException.ThrowIfNull(body);
        EndpointName = endpointName;
        Body = body;
        DeliverAt = deliverAt;
    }

    /// <summary>The broker endpoint the message is sent to.</summary>
    public string EndpointName { get; }

    /// <summary>The bytes sent: normally a NymBroker envelope.</summary>
#pragma warning disable CA1819 // the endpoints take the array as it is; a copy per message would be waste
    public byte[] Body { get; }
#pragma warning restore CA1819

    /// <summary>Not sent before this time; null sends it as soon as it is committed.</summary>
    public DateTimeOffset? DeliverAt { get; }
}

/// <summary>An outbox row claimed for sending.</summary>
public sealed class ClaimedOutboxMessage
{
    public ClaimedOutboxMessage(long id, string endpointName, byte[] body, DateTimeOffset? deliverAt, int attempt)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpointName);
        ArgumentNullException.ThrowIfNull(body);
        Id = id;
        EndpointName = endpointName;
        Body = body;
        DeliverAt = deliverAt;
        Attempt = attempt;
    }

    /// <summary>The row's key, increasing in insert order.</summary>
    public long Id { get; }

    public string EndpointName { get; }

#pragma warning disable CA1819 // see OutboxMessage.Body
    public byte[] Body { get; }
#pragma warning restore CA1819

    public DateTimeOffset? DeliverAt { get; }

    /// <summary>Which attempt this claim is: 1 the first time.</summary>
    public int Attempt { get; }
}
