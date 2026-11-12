using System.Diagnostics;
using NymBroker.Core.Diagnostics;
using NymBroker.Core.Message;

namespace NymBroker.Core.Impl;

// Distributed tracing across message boundaries (#116): a send activity per post, route or topic delivery whose
// W3C context goes into the envelope, and a process activity that continues the context a received envelope carries.
internal sealed partial class NymBrokerImpl
{
    private const string ProcessActivityName = "nymbroker.process";
    private const string SendActivityName = "nymbroker.send";

    private TracingOptions _tracing = new();

    internal void ConfigureTracing(TracingOptions options) => _tracing = options;

    /// <summary>
    /// The consumer activity for one received message: a child of the envelope's context, or with
    /// <see cref="TraceContinuation.Link"/> the root of a new trace linked to it. Without a valid context it is a child
    /// of the current activity, as before trace propagation.
    /// </summary>
    private Activity? StartProcessActivity(string? source, IMessageContext? received)
    {
        var activity = TraceContext.TryGetRemote(received?.TraceParent, received?.TraceState, out var remote)
            ? StartContinuedActivity(remote)
            : NymBrokerActivitySource.Source.StartActivity(ProcessActivityName, ActivityKind.Consumer);

        activity?.SetTag("messaging.system", "nymbroker");
        activity?.SetTag("messaging.operation.type", "process");
        activity?.SetTag("nymbroker.source", source);
        if (source is not null) activity?.SetTag("messaging.destination.name", source);
        return activity;
    }

    private Activity? StartContinuedActivity(ActivityContext remote)
    {
        if (_tracing.Continuation == TraceContinuation.Parent)
            return NymBrokerActivitySource.Source.StartActivity(ProcessActivityName, ActivityKind.Consumer, remote);

        // A new trace: no parent at all, not even the listener loop's ambient activity. ProcessAsync is async, so
        // clearing Activity.Current here does not reach its caller.
        var ambient = Activity.Current;
        Activity.Current = null;
        var activity = NymBrokerActivitySource.Source.StartActivity(
            ProcessActivityName, ActivityKind.Consumer, default(ActivityContext), links: [new ActivityLink(remote)]);
        if (activity is null) Activity.Current = ambient;
        return activity;
    }

    /// <summary>
    /// The producer activity for sending <paramref name="count"/> message(s) to <paramref name="destination"/>; the
    /// envelopes serialized while it is current carry its context.
    /// </summary>
    private static Activity? StartSendActivity(string destination, IMessageContext? context, int count = 1)
    {
        var activity = NymBrokerActivitySource.Source.StartActivity(SendActivityName, ActivityKind.Producer);
        if (activity is null) return null;

        activity.SetTag("messaging.system", "nymbroker");
        activity.SetTag("messaging.operation.type", "send");
        activity.SetTag("messaging.destination.name", destination);
        if (count != 1)
        {
            activity.SetTag("messaging.batch.message_count", count);
        }
        else if (context is not null)
        {
            activity.SetTag("messaging.message.id", context.Id);
            activity.SetTag("messaging.conversation_id", context.CorrelationId);
            activity.SetTag("messaging.message.type", context.MessageType);
        }
        return activity;
    }

    private static void RecordSendFailure(Activity? activity, Exception exception)
    {
        activity?.SetTag("error.type", exception.GetType().FullName);
        activity?.SetStatus(ActivityStatusCode.Error);
    }

    /// <summary>Serializes a new envelope with the current trace context (see <see cref="TraceContext.Outgoing"/>).</summary>
    private byte[] SerializeOutgoing(IMessageContext context)
    {
        if (TraceContext.Outgoing(_tracing.PropagateTraceContext) is { } stamp && context is ITraceCarrier carrier)
        {
            carrier.TraceParent = stamp.TraceParent;
            carrier.TraceState = stamp.TraceState;
        }
        return _serializer.SerializeRegistered(context);
    }

    /// <summary>As <see cref="SerializeOutgoing"/> for a received context, whose own trace fields are restored afterwards.</summary>
    private byte[] SerializeOutgoingKeepingContext(IMessageContext context)
    {
        if (context is not ITraceCarrier carrier) return _serializer.SerializeRegistered(context);

        var (parent, state) = (carrier.TraceParent, carrier.TraceState);
        try
        {
            return SerializeOutgoing(context);
        }
        finally
        {
            (carrier.TraceParent, carrier.TraceState) = (parent, state);
        }
    }
}
