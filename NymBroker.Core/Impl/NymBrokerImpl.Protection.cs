using System.Collections.Frozen;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Message;
using NymBroker.Core.Protection;
using NymBroker.Core.Serialize;

namespace NymBroker.Core.Impl;

/// <summary>Payload protection (#135): the payload of envelopes sent to the listed endpoints is encrypted; received ones are decrypted in <see cref="Decode"/>.</summary>
internal sealed partial class NymBrokerImpl
{
    private IPayloadProtector? _payloadProtector;
    private FrozenSet<string> _protectedEndpoints = FrozenSet<string>.Empty;
    private FrozenSet<string> _protectionRequiredEndpoints = FrozenSet<string>.Empty;

    public void SetPayloadProtection(IPayloadProtector protector, IEnumerable<string> protectedEndpoints, IEnumerable<string> requiredOnReceive)
    {
        ArgumentNullException.ThrowIfNull(protector);
        _payloadProtector = protector;
        _protectedEndpoints = protectedEndpoints.ToFrozenSet(StringComparer.OrdinalIgnoreCase);
        _protectionRequiredEndpoints = requiredOnReceive.ToFrozenSet(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>The envelope with its payload protected when <paramref name="name"/> is a protected endpoint; otherwise the same bytes.</summary>
    private byte[] ProtectFor(string name, byte[] envelope)
        => _payloadProtector is { } protector && _protectedEndpoints.Contains(name)
            ? PayloadEnvelope.Protect(envelope, protector, name)
            : envelope;

    /// <summary>
    /// Decrypts a protected payload, or rejects plaintext on an endpoint that requires protection. A reassembled split
    /// message is not checked for the requirement: its parts were.
    /// </summary>
    /// <exception cref="PayloadProtectionException">The message must be dead-lettered.</exception>
    private void UnprotectReceived(IMessageContext context, string? sourceEndpoint, bool reassembled)
    {
        if (context is RawMessageContext { Protection: not null } raw)
        {
            PayloadEnvelope.Unprotect(raw, _payloadProtector);
            return;
        }

        if (!reassembled && sourceEndpoint is not null && _protectionRequiredEndpoints.Contains(sourceEndpoint))
            throw new PayloadProtectionException(DeadLetterReasons.PayloadNotProtected,
                $"Endpoint '{sourceEndpoint}' requires protected payloads, and this one is plaintext.");
    }
}
