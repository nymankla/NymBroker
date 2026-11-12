namespace NymBroker.Core.Diagnostics;

/// <summary>How a consumer's <c>nymbroker.process</c> activity continues the trace context carried in the envelope.</summary>
public enum TraceContinuation
{
    /// <summary>
    /// The producer's context is the parent: producer and consumer share one trace. Best for request/response-like
    /// flows and what most trace viewers show best.
    /// </summary>
    Parent,

    /// <summary>
    /// Each processed message starts a new trace that links to the producer's context (the OpenTelemetry messaging
    /// recommendation for long-running or fan-out flows, where one trace would grow without bound).
    /// </summary>
    Link
}

/// <summary>W3C trace context propagation through the envelope, set with <c>NymBrokerBuilder.ConfigureTracing</c>.</summary>
public sealed class TracingOptions
{
    /// <summary>
    /// Write <c>traceParent</c> / <c>traceState</c> into the envelopes the broker sends (posts, publishes, routes, topics,
    /// dead letters). Default true. Turn it off when messages leave your trust boundary and must not reveal trace ids or
    /// vendor trace state; the broker then also drops a received message's context when it forwards the message.
    /// Received context is always read.
    /// </summary>
    public bool PropagateTraceContext { get; set; } = true;

    /// <summary>How a received message's context becomes the consumer activity's parent or link. Default <see cref="TraceContinuation.Parent"/>.</summary>
    public TraceContinuation Continuation { get; set; } = TraceContinuation.Parent;
}
