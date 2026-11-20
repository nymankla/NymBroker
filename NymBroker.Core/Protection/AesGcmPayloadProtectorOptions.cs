namespace NymBroker.Core.Protection;

/// <summary>
/// The key ring of an <see cref="AesGcmPayloadProtector"/>: every key a receiver may meet, and the one new payloads use.
/// Rotate by adding the new key everywhere first, then making it current on the senders, and removing the old one when
/// no message encrypted with it is left. Load keys from a secret store, never from source code.
/// </summary>
public sealed class AesGcmPayloadProtectorOptions
{
    private readonly Dictionary<string, byte[]> _keys = new(StringComparer.Ordinal);

    /// <summary>The id of the key that encrypts new payloads; must be one of the added keys.</summary>
    public string? CurrentKeyId { get; set; }

    /// <summary>The ids of the added keys; the keys themselves are not exposed.</summary>
    public IReadOnlyCollection<string> KeyIds => _keys.Keys;

    internal IReadOnlyDictionary<string, byte[]> Keys => _keys;

    /// <summary>Adds a 128-, 192- or 256-bit key (16, 24 or 32 bytes). The id travels in every envelope, so it must not be secret.</summary>
    public AesGcmPayloadProtectorOptions AddKey(string keyId, ReadOnlySpan<byte> key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyId);
        if (key.Length is not (16 or 24 or 32))
            throw new ArgumentException($"An AES key is 16, 24 or 32 bytes; key '{keyId}' has {key.Length}.", nameof(key));
        if (!_keys.TryAdd(keyId, key.ToArray()))
            throw new ArgumentException($"A key with id '{keyId}' was added already.", nameof(keyId));
        return this;
    }

    /// <summary>Adds a key given as Base64, e.g. from configuration or a secret store. See <see cref="AddKey"/>.</summary>
    public AesGcmPayloadProtectorOptions AddKeyFromBase64(string keyId, string base64Key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(base64Key);
        return AddKey(keyId, Convert.FromBase64String(base64Key));
    }

    internal void Validate()
    {
        if (_keys.Count == 0)
            throw new InvalidOperationException("AES-GCM payload protection needs at least one key (AddKey).");
        if (string.IsNullOrWhiteSpace(CurrentKeyId))
            throw new InvalidOperationException("AES-GCM payload protection needs a CurrentKeyId: the key that encrypts new payloads.");
        if (!_keys.ContainsKey(CurrentKeyId))
            throw new InvalidOperationException($"CurrentKeyId '{CurrentKeyId}' is not one of the added keys.");
    }
}
