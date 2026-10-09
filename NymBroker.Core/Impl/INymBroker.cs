using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Filter;
using NymBroker.Core.Message;
using NymBroker.Core.Route;
using NymBroker.Core.Transform;

namespace NymBroker.Core.Impl;

public interface INymBroker
{
    /// <summary>
    /// Serialize and post a typed message to a named endpoint.
    /// When <paramref name="splitThresholdBytes"/> is set and the serialized envelope exceeds it,
    /// the message is transparently split into <see cref="Aggregator.SplitMessage"/> parts (via
    /// <see cref="Splitter.ISplitter"/>) and each part is posted individually; the receiving side
    /// reassembles them automatically inside <c>ProcessAsync</c> once all parts arrive.
    /// When <paramref name="compress"/> is also true (the default), the payload is compressed
    /// (<see cref="Splitter.ICompressor"/>) before splitting whenever that actually reduces its
    /// size — this typically more than offsets the ~33% Base64 overhead of splitting for
    /// compressible (text/JSON) payloads. Ignored when <paramref name="splitThresholdBytes"/> is null.
    /// </summary>
    Task PostAsync<T>(string endpointName, T message, CancellationToken ct = default, int? splitThresholdBytes = null, bool compress = true) where T : class;

    /// <summary>
    /// Post a pre-serialized stream to a named endpoint.
    /// When <paramref name="splitThresholdBytes"/> is set and the stream exceeds it, the payload
    /// is transparently split into <see cref="Aggregator.SplitMessage"/> parts and posted individually.
    /// See <see cref="PostAsync{T}"/> for the <paramref name="compress"/> behavior.
    /// </summary>
    Task PostAsync(string endpointName, Stream messageStream, CancellationToken ct = default, int? splitThresholdBytes = null, bool compress = true);

    /// <summary>Start a fluent route definition for message type T.</summary>
    IRouteBuilder<T> Route<T>() where T : class;

    /// <summary>Start a fluent route definition that matches any message type.</summary>
    IRouteBuilder<IAnyMessage> Route();

    /// <summary>Register a route from a custom route builder.</summary>
    RouteContext Route(IRouteBuilder routeBuilder);

    /// <summary>Start a route definition backed by a custom route context factory.</summary>
    IRouteBuilder<IAnyMessage> Route(Func<RouteContext> routeContextFactory);

    INymBroker AddScheduledAction(TimeSpan timeSpan, Action action);
    INymBroker AddScheduledAction<T1>(TimeSpan timeSpan, Action<T1> action, T1 param1);
    INymBroker AddScheduledAction<T1, T2>(TimeSpan timeSpan, Action<T1, T2> action, T1 param1, T2 param2);
    INymBroker AddScheduledAction<T1>(string expression, Action<T1> action, T1 param1);

    INymBroker AddFilter(IMessageFilter filter);

    /// <summary>
    /// Register an input transformer for a specific endpoint, or globally when endpoint is null.
    /// The transformer replaces the default JSON-envelope deserialization for matching messages.
    /// </summary>
    INymBroker AddInputTransformer(IInputTransformer transformer, string? endpoint = null);

    /// <summary>
    /// Forward messages to this endpoint when a consumer throws.
    /// Also receives messages that exceed the configured max message age.
    /// </summary>
    INymBroker SetDeadLetterEndpoint(string endpointName);

    /// <summary>Copy every incoming raw message to this endpoint before processing.</summary>
    INymBroker AddWireTap(string endpointName);

    /// <summary>Discard (and dead-letter if configured) messages older than maxAge.</summary>
    INymBroker SetMaxMessageAge(TimeSpan maxAge);

    Task StartAsync(CancellationToken ct = default);
    Task StopAsync(CancellationToken ct = default);

    /// <summary>
    /// Checks every registered endpoint (in parallel, on the thread pool) and the broker's own state, and returns one
    /// aggregated <see cref="BrokerHealthReport"/>. An endpoint that does not answer within
    /// <see cref="BrokerHealthCheckOptions.Timeout"/> (default 10 s), or whose check throws, is reported unhealthy.
    /// Never throws, except <see cref="OperationCanceledException"/> when <paramref name="ct"/> is cancelled.
    /// </summary>
    Task<BrokerHealthReport> CheckHealthAsync(CancellationToken ct = default);

    /// <summary>
    /// Serialize and post several messages to one endpoint in a single endpoint call (<see cref="IEndPoint.PostBatchAsync"/>).
    /// Each message gets its own envelope and is received and settled on its own; order is preserved.
    /// <paramref name="splitThresholdBytes"/> / <paramref name="compress"/> apply per message, as in <c>PostAsync</c>.
    /// <paramref name="correlationId"/>, when given, is set on every envelope so the batch can be traced.
    /// Whether the batch is atomic depends on the transport (see the endpoint's documentation).
    /// </summary>
    Task PostBatchAsync<T>(string endpointName, IEnumerable<T> messages, CancellationToken ct = default,
        int? splitThresholdBytes = null, bool compress = true, Guid? correlationId = null) where T : class;

    /// <summary>Publish a message to all topics matching its CLR type.</summary>
    Task PublishAsync<T>(T message, CancellationToken ct = default) where T : class;

    /// <summary>Publish a message directly to a named topic, bypassing type-based topic matching.</summary>
    Task PublishAsync<T>(string topicName, T message, CancellationToken ct = default) where T : class;

    /// <summary>
    /// Publish several messages, one at a time through the same path as <see cref="PublishAsync{T}(T, CancellationToken)"/>
    /// (routes, topics and consumers are evaluated per message). Not atomic: a failure surfaces after the messages before it.
    /// </summary>
    Task PublishBatchAsync<T>(IEnumerable<T> messages, CancellationToken ct = default) where T : class;

    /// <summary>
    /// Publish several messages to a named topic. Copies for each subscriber endpoint are sent with one
    /// <see cref="IEndPoint.PostBatchAsync"/> call; <c>ISubscribe&lt;T&gt;</c> subscribers receive the messages one by one.
    /// </summary>
    Task PublishBatchAsync<T>(string topicName, IEnumerable<T> messages, CancellationToken ct = default) where T : class;

    /// <summary>
    /// Process raw UTF-8 JSON bytes arriving from an endpoint. Called by endpoint listeners, which settle the message
    /// according to the returned <see cref="ProcessResult"/>. Never throws except <see cref="OperationCanceledException"/> when
    /// <paramref name="ct"/> is cancelled; a cancellation raised by a consumer itself (e.g. an HTTP timeout) is a failure like any other.
    /// </summary>
    Task<ProcessResult> ProcessAsync(byte[] raw, string? sourceEndpoint = null, CancellationToken ct = default);

    /// <summary>Convenience overload for tests and external callers; converts the string to UTF-8 bytes.</summary>
    Task<ProcessResult> ProcessAsync(string raw, string? sourceEndpoint = null, CancellationToken ct = default);
}
