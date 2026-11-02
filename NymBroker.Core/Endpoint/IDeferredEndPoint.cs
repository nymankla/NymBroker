namespace NymBroker.Core.Endpoint;

/// <summary>
/// An endpoint that can hold a message back until a given time. For an endpoint that does not implement this,
/// <c>INymBroker.PostDeferredAsync</c> holds the message in the broker's memory instead, which is not durable, and the
/// broker logs a warning when the endpoint is added.
/// </summary>
public interface IDeferredEndPoint : IEndPoint
{
    /// <summary>
    /// Sends one envelope that must not be delivered before <paramref name="deliverAt"/>; a time in the past means now.
    /// Other messages on the endpoint are not held up. Never delivers early. The first delivery counts as the first attempt.
    /// </summary>
    /// <exception cref="NotSupportedException">
    /// The transport cannot honour this message or time (for example a deferral longer than it allows). The broker then
    /// holds the message in memory, as for an endpoint that does not implement this interface. The broker cannot know of
    /// this when the endpoint is added, so an endpoint that does this should log its own warning when it is created.
    /// </exception>
    Task PostDeferredAsync(byte[] message, DateTimeOffset deliverAt, CancellationToken ct = default);
}
