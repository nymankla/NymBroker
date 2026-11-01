namespace NymBroker.Core.Endpoint;

/// <summary>
/// An endpoint that can hold a message back until a given time. <c>INymBroker.PostDeferredAsync</c> throws for an endpoint
/// that does not implement this, so a deferred message is never delivered early without saying so.
/// </summary>
public interface IDeferredEndPoint : IEndPoint
{
    /// <summary>
    /// Sends one envelope that must not be delivered before <paramref name="deliverAt"/>; a time in the past means now.
    /// Other messages on the endpoint are not held up. Throws if the transport cannot honour the time (for example a
    /// deferral longer than it allows), never delivers early. The first delivery counts as the first attempt.
    /// </summary>
    Task PostDeferredAsync(byte[] message, DateTimeOffset deliverAt, CancellationToken ct = default);
}
