namespace NymBroker.Dashboard;

/// <summary>Settings for the NymBroker dashboard, passed to <c>AddNymBrokerDashboard</c>.</summary>
public sealed class NymBrokerDashboardOptions
{
    /// <summary>Shown in the page header and the browser tab. Default <c>NymBroker</c>.</summary>
    public string Title { get; set; } = "NymBroker";

    /// <summary>
    /// How often open dashboards get new numbers: rates, latency and the queue counts of inspectable endpoints (one
    /// count query per endpoint per refresh, shared by every open dashboard). Default 2 s, at least 500 ms.
    /// </summary>
    public TimeSpan RefreshInterval { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>How often open dashboards run the broker's health check. Default 10 s, at least 1 s.</summary>
    public TimeSpan HealthInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>How much metric history the charts keep, in memory and per process. Default 15 minutes, 1 minute to 1 hour.</summary>
    public TimeSpan History { get; set; } = TimeSpan.FromMinutes(15);

    /// <summary>
    /// Whether the queue view shows message payloads. Null (default) shows them only when the host environment is
    /// Development: payloads can hold personal data.
    /// </summary>
    public bool? ShowPayloads { get; set; }

    /// <summary>Payloads longer than this are cut off in the queue view. Default 64 KB.</summary>
    public int MaxPayloadBytes { get; set; } = 64 * 1024;

    /// <summary>
    /// Masks a payload before it is shown: called with the endpoint name and the message bytes, returns the bytes to show.
    /// Use it to remove personal data while still showing payloads.
    /// </summary>
    public Func<string, ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>? RedactPayload { get; set; }

    /// <summary>
    /// Without an authorization policy on the dashboard (<c>MapNymBrokerDashboard(...).RequireAuthorization(...)</c>)
    /// it answers only requests from the local machine. Set this to answer anyone; only do so on a closed network.
    /// </summary>
    public bool AllowAnonymousRemoteAccess { get; set; }

    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(Title))
            throw new ArgumentException("Title must not be empty.", nameof(Title));
        if (RefreshInterval < TimeSpan.FromMilliseconds(500))
            throw new ArgumentOutOfRangeException(nameof(RefreshInterval), RefreshInterval, "RefreshInterval must be at least 500 ms.");
        if (HealthInterval < TimeSpan.FromSeconds(1))
            throw new ArgumentOutOfRangeException(nameof(HealthInterval), HealthInterval, "HealthInterval must be at least 1 s.");
        if (History < TimeSpan.FromMinutes(1) || History > TimeSpan.FromHours(1))
            throw new ArgumentOutOfRangeException(nameof(History), History, "History must be between 1 minute and 1 hour.");
        if (MaxPayloadBytes < 256)
            throw new ArgumentOutOfRangeException(nameof(MaxPayloadBytes), MaxPayloadBytes, "MaxPayloadBytes must be at least 256.");
    }
}
