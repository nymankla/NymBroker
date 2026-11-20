using System.Security.Cryptography;
using System.Text;
using NymBroker.Core.Protection;

namespace NymBroker.Tests;

/// <summary>The reference payload protector (#135): AES-GCM with a key ring.</summary>
public sealed class AesGcmPayloadProtectorTests
{
    private static readonly byte[] Payload = """{"secret":"4111-1111"}"""u8.ToArray();
    private static readonly byte[] AssociatedData = "envelope"u8.ToArray();

    internal static byte[] Key(byte seed, int length = 32) => Enumerable.Repeat(seed, length).ToArray();

    private static AesGcmPayloadProtector Protector(string current, params (string Id, byte[] Key)[] keys)
    {
        var options = new AesGcmPayloadProtectorOptions { CurrentKeyId = current };
        foreach (var (id, key) in keys) options.AddKey(id, key);
        return new AesGcmPayloadProtector(options);
    }

    [Theory]
    [InlineData(16)]
    [InlineData(24)]
    [InlineData(32)]
    public void Protect_ThenUnprotect_ReturnsThePayload(int keyLength)
    {
        var protector = Protector("k1", ("k1", Key(1, keyLength)));

        var protectedPayload = protector.Protect(Payload, AssociatedData);

        Assert.Equal(AesGcmPayloadProtector.AlgorithmName, protectedPayload.Algorithm);
        Assert.Equal("k1", protectedPayload.KeyId);
        Assert.Equal(12 + Payload.Length + 16, protectedPayload.Data.Length);
        Assert.Equal(Payload, protector.Unprotect(protectedPayload, AssociatedData));
    }

    [Fact]
    public void Protect_UsesANewNonceEachTime()
    {
        var protector = Protector("k1", ("k1", Key(1)));

        Assert.NotEqual(protector.Protect(Payload, AssociatedData).Data, protector.Protect(Payload, AssociatedData).Data);
    }

    [Fact]
    public void Protect_DoesNotContainThePlaintext()
    {
        var data = Protector("k1", ("k1", Key(1))).Protect(Payload, AssociatedData).Data;

        Assert.DoesNotContain("4111", Encoding.Latin1.GetString(data));
    }

    [Fact]
    public void Unprotect_WithAnotherKeyUnderTheSameId_Throws()
    {
        var protectedPayload = Protector("k1", ("k1", Key(1))).Protect(Payload, AssociatedData);

        Assert.ThrowsAny<CryptographicException>(() => Protector("k1", ("k1", Key(2))).Unprotect(protectedPayload, AssociatedData));
    }

    [Fact]
    public void Unprotect_UnknownKeyId_Throws()
    {
        var protectedPayload = Protector("k1", ("k1", Key(1))).Protect(Payload, AssociatedData);

        var ex = Assert.ThrowsAny<CryptographicException>(() => Protector("k2", ("k2", Key(1))).Unprotect(protectedPayload, AssociatedData));
        Assert.Contains("k1", ex.Message);
    }

    [Fact]
    public void Unprotect_ChangedData_Throws()
    {
        var protector = Protector("k1", ("k1", Key(1)));
        var protectedPayload = protector.Protect(Payload, AssociatedData);
        protectedPayload.Data[20] ^= 1;

        Assert.ThrowsAny<CryptographicException>(() => protector.Unprotect(protectedPayload, AssociatedData));
    }

    [Fact]
    public void Unprotect_ChangedAssociatedData_Throws()
    {
        var protector = Protector("k1", ("k1", Key(1)));
        var protectedPayload = protector.Protect(Payload, AssociatedData);

        Assert.ThrowsAny<CryptographicException>(() => protector.Unprotect(protectedPayload, "another envelope"u8));
    }

    [Fact]
    public void Unprotect_AnotherAlgorithm_Throws()
    {
        var protector = Protector("k1", ("k1", Key(1)));
        var protectedPayload = protector.Protect(Payload, AssociatedData) with { Algorithm = "ChaCha20" };

        Assert.ThrowsAny<CryptographicException>(() => protector.Unprotect(protectedPayload, AssociatedData));
    }

    [Fact]
    public void Unprotect_TooShort_Throws()
    {
        var protector = Protector("k1", ("k1", Key(1)));

        Assert.ThrowsAny<CryptographicException>(() => protector.Unprotect(new ProtectedPayload(AesGcmPayloadProtector.AlgorithmName, "k1", new byte[27]), AssociatedData));
    }

    [Fact]
    public void Rotation_TheOldKeyStillDecrypts_AndNewPayloadsUseTheCurrentKey()
    {
        var before = Protector("2026-10", ("2026-10", Key(1))).Protect(Payload, AssociatedData);
        var rotated = Protector("2026-11", ("2026-10", Key(1)), ("2026-11", Key(2)));

        Assert.Equal(Payload, rotated.Unprotect(before, AssociatedData));
        var after = rotated.Protect(Payload, AssociatedData);
        Assert.Equal("2026-11", after.KeyId);
        Assert.Equal(Payload, rotated.Unprotect(after, AssociatedData));
    }

    [Fact]
    public void AddKeyFromBase64_AddsTheDecodedKey()
    {
        var options = new AesGcmPayloadProtectorOptions { CurrentKeyId = "k1" }.AddKeyFromBase64("k1", Convert.ToBase64String(Key(7)));
        var protectedPayload = new AesGcmPayloadProtector(options).Protect(Payload, AssociatedData);

        Assert.Equal(Payload, Protector("k1", ("k1", Key(7))).Unprotect(protectedPayload, AssociatedData));
        Assert.Equal(["k1"], options.KeyIds);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(15)]
    [InlineData(31)]
    [InlineData(64)]
    public void AddKey_WrongLength_Throws(int length)
        => Assert.Throws<ArgumentException>(() => new AesGcmPayloadProtectorOptions().AddKey("k1", new byte[length]));

    [Fact]
    public void AddKey_SameIdTwice_Throws()
        => Assert.Throws<ArgumentException>(() => new AesGcmPayloadProtectorOptions().AddKey("k1", Key(1)).AddKey("k1", Key(2)));

    [Fact]
    public void Constructor_NoKeys_Throws()
        => Assert.Throws<InvalidOperationException>(() => new AesGcmPayloadProtector(new AesGcmPayloadProtectorOptions { CurrentKeyId = "k1" }));

    [Fact]
    public void Constructor_NoCurrentKey_Throws()
        => Assert.Throws<InvalidOperationException>(() => new AesGcmPayloadProtector(new AesGcmPayloadProtectorOptions().AddKey("k1", Key(1))));

    [Fact]
    public void Constructor_CurrentKeyNotAdded_Throws()
        => Assert.Throws<InvalidOperationException>(() => new AesGcmPayloadProtector(new AesGcmPayloadProtectorOptions { CurrentKeyId = "k2" }.AddKey("k1", Key(1))));
}
