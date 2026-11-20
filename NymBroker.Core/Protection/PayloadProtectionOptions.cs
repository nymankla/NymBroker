namespace NymBroker.Core.Protection;

/// <summary>Which endpoints get protected payloads (<c>NymBrokerBuilder.ProtectPayloads</c>).</summary>
public sealed class PayloadProtectionOptions
{
    private readonly HashSet<string> _protected = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _required = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Endpoints whose envelopes are sent with an encrypted payload.</summary>
    public IReadOnlyCollection<string> ProtectedEndpoints => _protected;

    /// <summary>Endpoints on which a plaintext payload is dead-lettered (<see cref="Endpoint.DeadLetterReasons.PayloadNotProtected"/>).</summary>
    public IReadOnlyCollection<string> RequiredOnReceive => _required;

    /// <summary>
    /// Protect the payload of every envelope the broker sends to these endpoints: posts, routes, topics, wire taps,
    /// dead letters, deferred and outbox sends. Envelopes already protected are sent unchanged.
    /// </summary>
    public PayloadProtectionOptions ForEndpoints(params string[] endpointNames)
        => AddAll(_protected, endpointNames);

    /// <summary>
    /// Dead-letter plaintext payloads received on these endpoints instead of processing them. Without it a receiver accepts
    /// both, so senders can be switched over one by one. A protected payload is decrypted on every endpoint.
    /// </summary>
    public PayloadProtectionOptions RequireOnReceive(params string[] endpointNames)
        => AddAll(_required, endpointNames);

    private PayloadProtectionOptions AddAll(HashSet<string> set, string[] endpointNames)
    {
        ArgumentNullException.ThrowIfNull(endpointNames);
        foreach (var name in endpointNames)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(name, nameof(endpointNames));
            set.Add(name);
        }
        return this;
    }
}
