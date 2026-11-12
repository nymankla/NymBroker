using System.Diagnostics;

namespace NymBroker.Core.Diagnostics;

/// <summary>A context whose trace fields the broker sets when it sends it (both envelope types implement it).</summary>
internal interface ITraceCarrier
{
    string? TraceParent { get; set; }
    string? TraceState { get; set; }
}

/// <summary>The <c>traceParent</c> / <c>traceState</c> pair to write into an envelope.</summary>
internal readonly record struct TraceStamp(string? TraceParent, string? TraceState);

/// <summary>Reading and writing W3C trace context in envelopes (#116). Never throws on bad input: it is ignored.</summary>
internal static class TraceContext
{
    /// <summary>The W3C limit for <c>tracestate</c>; a longer one is dropped rather than cut (cutting could split a member).</summary>
    internal const int MaxTraceStateLength = 512;

    /// <summary>
    /// The received pair as it may be carried on: <c>traceParent</c> only if it parses as a W3C traceparent, and
    /// <c>traceState</c> only with a valid traceparent and within <see cref="MaxTraceStateLength"/>.
    /// </summary>
    internal static TraceStamp Sanitize(string? traceParent, string? traceState)
    {
        if (traceParent is null || !ActivityContext.TryParse(traceParent, null, isRemote: true, out _))
            return default;
        return new TraceStamp(traceParent, traceState is { Length: > 0 and <= MaxTraceStateLength } ? traceState : null);
    }

    /// <summary>The remote parent carried by <paramref name="traceParent"/>, if it is valid.</summary>
    internal static bool TryGetRemote(string? traceParent, string? traceState, out ActivityContext context)
    {
        context = default;
        return traceParent is not null
               && ActivityContext.TryParse(traceParent, traceState is { Length: <= MaxTraceStateLength } ? traceState : null, isRemote: true, out context);
    }

    /// <summary>
    /// What an outgoing envelope should carry: the current activity's context; nothing when propagation is off; or null
    /// (keep the envelope's own) when there is no current W3C activity, so context passes through hops nobody traces.
    /// </summary>
    internal static TraceStamp? Outgoing(bool propagate)
    {
        if (!propagate) return default(TraceStamp);
        return Activity.Current is { IdFormat: ActivityIdFormat.W3C } current
            ? new TraceStamp(current.Id, current.TraceStateString is { Length: <= MaxTraceStateLength } state ? state : null)
            : null;
    }
}
