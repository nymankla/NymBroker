using System.Collections.Frozen;
using System.Security.Cryptography;

namespace NymBroker.Core.Protection;

/// <summary>
/// AES-GCM payload protection (authenticated encryption, so a changed payload or envelope is detected). Encrypts with the
/// options' current key; decrypts with whichever key of the key ring the envelope names, so keys can be rotated.
/// <see cref="ProtectedPayload.Data"/> is a 12-byte random nonce, the ciphertext and a 16-byte tag.
/// </summary>
public sealed class AesGcmPayloadProtector : IPayloadProtector
{
    /// <summary>The value of <see cref="Algorithm"/>, written to the envelope's <c>protection.alg</c>.</summary>
    public const string AlgorithmName = "AES-GCM";

    private const int NonceSize = 12;
    private const int TagSize = 16;

    private readonly FrozenDictionary<string, byte[]> _keys;
    private readonly string _currentKeyId;
    private readonly byte[] _currentKey;

    public AesGcmPayloadProtector(AesGcmPayloadProtectorOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _keys = options.Keys.ToFrozenDictionary(StringComparer.Ordinal);
        _currentKeyId = options.CurrentKeyId!;
        _currentKey = _keys[_currentKeyId];
    }

    public string Algorithm => AlgorithmName;

    /// <summary>The key id new payloads are encrypted with.</summary>
    public string CurrentKeyId => _currentKeyId;

    public ProtectedPayload Protect(ReadOnlySpan<byte> payload, ReadOnlySpan<byte> associatedData)
    {
        var data = new byte[NonceSize + payload.Length + TagSize];
        var nonce = data.AsSpan(0, NonceSize);
        RandomNumberGenerator.Fill(nonce);

        using var aes = new AesGcm(_currentKey, TagSize);
        aes.Encrypt(nonce, payload, data.AsSpan(NonceSize, payload.Length), data.AsSpan(NonceSize + payload.Length), associatedData);
        return new ProtectedPayload(AlgorithmName, _currentKeyId, data);
    }

    public byte[] Unprotect(ProtectedPayload payload, ReadOnlySpan<byte> associatedData)
    {
        ArgumentNullException.ThrowIfNull(payload);
        if (payload.Algorithm != AlgorithmName)
            throw new CryptographicException($"The payload uses algorithm '{payload.Algorithm}', not {AlgorithmName}.");
        if (!_keys.TryGetValue(payload.KeyId, out var key))
            throw new CryptographicException($"No key with id '{payload.KeyId}' in the key ring.");
        if (payload.Data.Length < NonceSize + TagSize)
            throw new CryptographicException("The protected payload is too short.");

        var data = payload.Data.AsSpan();
        var plaintext = new byte[data.Length - NonceSize - TagSize];
        using var aes = new AesGcm(key, TagSize);
        aes.Decrypt(data[..NonceSize], data.Slice(NonceSize, plaintext.Length), data[^TagSize..], plaintext, associatedData);
        return plaintext;
    }
}
