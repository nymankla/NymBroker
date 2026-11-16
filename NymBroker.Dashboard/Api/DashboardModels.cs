using NymBroker.Core.Description;
using NymBroker.Core.Endpoint.HealthCheck;
using NymBroker.Core.Endpoint.Queue;
using NymBroker.Dashboard.Metrics;

namespace NymBroker.Dashboard.Api;

/// <summary>Everything the page needs when it opens: configuration, history and the current numbers.</summary>
internal sealed record DashboardOverview(string Title, string Version, int RefreshMs, int HistorySeconds, bool ShowPayloads, BrokerDescription Broker,
    IReadOnlyList<MetricPoint> History, LiveSnapshot Live);

/// <summary>What open dashboards receive every refresh, over Server-Sent Events.</summary>
internal sealed record LiveSnapshot(DateTimeOffset At, bool Started, RecentRates Rates, IReadOnlyList<MetricPoint> Points,
    IReadOnlyList<EndpointLive> Endpoints, HealthView? Health);

/// <summary>One endpoint's numbers: rates from the metrics, counts from its queue inspector (inspectable endpoints only).</summary>
internal sealed record EndpointLive(string Name, EndpointRates Rates, QueueCounts? Counts, double? CountMs, string? CountError);

/// <summary>The latest health report and when it was taken.</summary>
internal sealed record HealthView(DateTimeOffset CheckedAt, BrokerHealthReport Report);

/// <summary>One page of an endpoint's queue.</summary>
internal sealed record MessagePageView(IReadOnlyList<MessageView> Messages, string? NextCursor, bool PayloadsShown);

/// <summary>A queued message as the queue view shows it.</summary>
internal sealed record MessageView(string Id, QueueInspectionState State, int Attempts, DateTimeOffset CreatedAt, DateTimeOffset? DeliverAt,
    DateTimeOffset? FailedAt, string? LastError, int Size, EnvelopeView? Envelope, string? Payload, bool PayloadTruncated);

/// <summary>The envelope fields of a message, when its bytes are a NymBroker envelope.</summary>
internal sealed record EnvelopeView(string? Id, string? CorrelationId, string? MessageType, string? Created, string? From, string? To,
    string? TraceParent, DeadLetterView? DeadLetter);

/// <summary>Why a message was dead-lettered, from the envelope's <c>deadLetter</c> object.</summary>
internal sealed record DeadLetterView(string? Reason, string? Description, string? ExceptionType, string? SourceEndpoint, string? DeadLetteredAt,
    int? DeliveryCount);

/// <summary>An error the page can show.</summary>
internal sealed record ProblemView(string Error);
