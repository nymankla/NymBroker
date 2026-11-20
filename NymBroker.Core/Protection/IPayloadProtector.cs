namespace NymBroker.Core.Protection;

/// <summary>
/// Encrypts and decrypts the <c>message</c> field of envelopes sent to the endpoints named in
/// <see cref="PayloadProtectionOptions.ForEndpoints"/> (registered with <c>NymBrokerBuilder.ProtectPayloads</c>). The rest of the
/// envelope (id, type, address, timestamps, trace context) stays readable. Implementations must be thread-safe.
/// </summary>
public interface IPayloadProtector
{
    /// <summary>Written to the envelope's <c>protection.alg</c>; a receiver only decrypts payloads whose algorithm matches its own.</summary>
    string Algorithm { get; }

    /// <summary>Encrypts <paramref name="payload"/> (the UTF-8 JSON of the message), authenticating <paramref name="associatedData"/> with it.</summary>
    /// <param name="associatedData">Binds the ciphertext to its envelope (message id, correlation id and type); not encrypted.</param>
    ProtectedPayload Protect(ReadOnlySpan<byte> payload, ReadOnlySpan<byte> associatedData);

    /// <summary>
    /// Decrypts <paramref name="payload"/> back to the message's UTF-8 JSON. Throws when it cannot: unknown key id, wrong key,
    /// or data or associated data that was changed. The broker dead-letters such a message as
    /// <see cref="Endpoint.DeadLetterReasons.PayloadUnprotectFailed"/>.
    /// </summary>
    byte[] Unprotect(ProtectedPayload payload, ReadOnlySpan<byte> associatedData);
}

/// <summary>A protected payload: the algorithm and key id travel in the envelope's <c>protection</c> block, the data in <c>message</c> (Base64).</summary>
/// <param name="Algorithm">The protector's <see cref="IPayloadProtector.Algorithm"/>.</param>
/// <param name="KeyId">Identifies the key, so a receiver can pick it from its key ring (rotation).</param>
/// <param name="Data">The protector's output, e.g. nonce, ciphertext and tag.</param>
public sealed record ProtectedPayload(string Algorithm, string KeyId, byte[] Data);
