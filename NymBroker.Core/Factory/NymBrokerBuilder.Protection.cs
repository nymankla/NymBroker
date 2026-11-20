using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NymBroker.Core.Protection;

namespace NymBroker.Core.Factory;

public sealed partial class NymBrokerBuilder
{
    private PayloadProtectionOptions? _payloadProtection;

    /// <summary>
    /// Protects the payload of envelopes sent to the endpoints named in <see cref="PayloadProtectionOptions.ForEndpoints"/>
    /// with <paramref name="protector"/>, e.g. an <see cref="AesGcmPayloadProtector"/>, and decrypts protected payloads on
    /// every endpoint. The rest of the envelope stays readable. A message that cannot be decrypted is dead-lettered
    /// (<see cref="Endpoint.DeadLetterReasons.PayloadUnprotectFailed"/>). One protector per broker; the last call wins.
    /// </summary>
    public NymBrokerBuilder ProtectPayloads(IPayloadProtector protector, Action<PayloadProtectionOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(protector);
        return ProtectPayloads(_ => protector, configure);
    }

    /// <summary>
    /// As <see cref="ProtectPayloads(IPayloadProtector, Action{PayloadProtectionOptions})"/>, with the protector created from
    /// DI once (singleton), e.g. with keys from a secret store.
    /// </summary>
    public NymBrokerBuilder ProtectPayloads(Func<IServiceProvider, IPayloadProtector> factory, Action<PayloadProtectionOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(configure);
        var options = new PayloadProtectionOptions();
        configure(options);

        _services.RemoveAll<IPayloadProtector>();
        _services.AddSingleton(factory);
        _payloadProtection = options;
        return this;
    }

    private void ValidatePayloadProtection()
    {
        if (_payloadProtection is null) return;

        var unknown = _payloadProtection.ProtectedEndpoints.Concat(_payloadProtection.RequiredOnReceive)
            .Where(n => !_endpoints.Contains(n, StringComparer.OrdinalIgnoreCase))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (unknown.Count > 0)
            throw new InvalidOperationException(
                $"ProtectPayloads names unknown endpoint(s): {string.Join(", ", unknown)}. Register the endpoint, or remove it from the protection options.");

        // An output transformer's bytes are not an envelope, so there is no payload field to protect.
        var transformed = _payloadProtection.ProtectedEndpoints
            .Where(n => _outputTransformers.Any(t => string.Equals(t.Endpoint, n, StringComparison.OrdinalIgnoreCase)))
            .ToList();
        if (transformed.Count > 0)
            throw new InvalidOperationException(
                $"Endpoint(s) {string.Join(", ", transformed)} have an output transformer and cannot protect payloads: the transformed bytes are not a NymBroker envelope.");

        // An input transformer reads a foreign format that never carries a protected NymBroker payload.
        var hasGlobalInputTransformer = _transformers.Any(t => t.Endpoint is null);
        var inputTransformed = _payloadProtection.RequiredOnReceive
            .Where(n => hasGlobalInputTransformer || _transformers.Any(t => string.Equals(t.Endpoint, n, StringComparison.OrdinalIgnoreCase)))
            .ToList();
        if (inputTransformed.Count > 0)
            throw new InvalidOperationException(
                $"Endpoint(s) {string.Join(", ", inputTransformed)} have an input transformer and cannot require protected payloads: they do not receive NymBroker envelopes.");
    }
}
