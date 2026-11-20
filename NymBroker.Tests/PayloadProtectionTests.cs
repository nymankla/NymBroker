using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using NymBroker.Core;
using NymBroker.Core.DI;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Endpoint.Memory;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;
using NymBroker.Core.Protection;
using NymBroker.Core.Serialize;
using NymBroker.Core.Transform;

namespace NymBroker.Tests;

/// <summary>Payload protection in the broker (#135): encrypted on send to listed endpoints, decrypted on receive.</summary>
public sealed class PayloadProtectionTests
{
    private const string Secret = "4111-1111-1111-1111";

    private sealed class Order
    {
        public string Id { get; set; } = "";
        public string Card { get; set; } = "";
    }

    private sealed class Node
    {
        public required NymBrokerImpl Broker { get; init; }
        public required Dictionary<string, MemoryQueueEndPoint> Endpoints { get; init; }
        public MemoryQueueEndPoint this[string name] => Endpoints[name];
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static IPayloadProtector Aes(string current, params (string Id, byte Seed)[] keys)
    {
        var options = new AesGcmPayloadProtectorOptions { CurrentKeyId = current };
        foreach (var (id, seed) in keys) options.AddKey(id, AesGcmPayloadProtectorTests.Key(seed));
        return new AesGcmPayloadProtector(options);
    }

    /// <summary>
    /// A broker with WriteOnly Memory endpoints Out (protected), Plain, Dlq and Tap, plus In (where messages are "received").
    /// Routes Orders received on In to <paramref name="routeTo"/>.
    /// </summary>
    private static Node CreateNode(IPayloadProtector? protector, string routeTo = "Plain", string[]? protect = null, string[]? require = null)
    {
        var broker = TestBroker.Create(new ServiceCollection().AddLogging().BuildServiceProvider());
        var endpoints = new Dictionary<string, MemoryQueueEndPoint>();
        foreach (var name in new[] { "Out", "Plain", "Dlq", "Tap" })
        {
            endpoints[name] = new MemoryQueueEndPoint(name, mode: EndpointMode.WriteOnly);
            broker.AddEndpoint(name, endpoints[name]);
        }
        broker.AddEndpoint("In", new MemoryQueueEndPoint("In"));
        broker.SetDeadLetterEndpoint("Dlq");
        broker.Route<Order>().To(routeTo).WhenFrom("In").Build();
        if (protector is not null)
            broker.SetPayloadProtection(protector, protect ?? ["Out"], require ?? []);
        return new Node { Broker = broker, Endpoints = endpoints };
    }

    private static async Task<List<string>> DrainAsync(MemoryQueueEndPoint endpoint)
    {
        var items = new List<string>();
        await foreach (var item in endpoint.ReadAsync(Ct)) items.Add(item);
        return items;
    }

    /// <summary>Sends an Order through <paramref name="sender"/> to its protected Out endpoint and returns the wire bytes.</summary>
    private static async Task<byte[]> SendProtectedAsync(Node sender, string id = "A-1")
    {
        await sender.Broker.PostAsync("Out", new Order { Id = id, Card = Secret }, Ct);
        return Encoding.UTF8.GetBytes(Assert.Single(await DrainAsync(sender["Out"])));
    }

    private static string PlaintextEnvelope(string card = Secret) =>
        $$$"""{"id":"{{{Guid.NewGuid()}}}","correlationId":"{{{Guid.NewGuid()}}}","messageType":"{{{MessageTypeName.Get(typeof(Order))}}}","created":"{{{DateTime.UtcNow:O}}}","message":{"id":"P-1","card":"{{{card}}}"}}""";

    private static async Task<string> DeadLetterReasonAsync(Node node)
    {
        using var dead = JsonDocument.Parse(Assert.Single(await DrainAsync(node["Dlq"])));
        return dead.RootElement.GetProperty("deadLetter").GetProperty("reason").GetString()!;
    }

    // --- Send ---

    [Fact]
    public async Task Post_ToAProtectedEndpoint_EncryptsOnlyThePayload()
    {
        var wire = await SendProtectedAsync(CreateNode(Aes("k1", ("k1", 1))));

        Assert.DoesNotContain(Secret, Encoding.UTF8.GetString(wire));
        using var envelope = JsonDocument.Parse(wire);
        var root = envelope.RootElement;
        Assert.Equal(AesGcmPayloadProtector.AlgorithmName, root.GetProperty("protection").GetProperty("alg").GetString());
        Assert.Equal("k1", root.GetProperty("protection").GetProperty("kid").GetString());
        Assert.Equal(JsonValueKind.String, root.GetProperty("message").ValueKind);
        // The rest stays readable.
        Assert.Equal(MessageTypeName.Get(typeof(Order)), root.GetProperty("messageType").GetString());
        Assert.Equal("Out", root.GetProperty("address").GetProperty("to").GetString());
        Assert.True(root.GetProperty("id").TryGetGuid(out _));
    }

    [Fact]
    public async Task Post_ToAnUnprotectedEndpoint_IsUnchanged()
    {
        var node = CreateNode(Aes("k1", ("k1", 1)));

        await node.Broker.PostAsync("Plain", new Order { Id = "A-1", Card = Secret }, Ct);

        var wire = Assert.Single(await DrainAsync(node["Plain"]));
        Assert.Contains(Secret, wire);
        using var envelope = JsonDocument.Parse(wire);
        Assert.False(envelope.RootElement.TryGetProperty("protection", out _));
    }

    [Fact]
    public async Task PostBatch_And_PostDeferred_AreProtected()
    {
        var node = CreateNode(Aes("k1", ("k1", 1)));

        await node.Broker.PostBatchAsync("Out", [new Order { Card = Secret }, new Order { Card = Secret }], Ct);
        await node.Broker.PostDeferredAsync("Out", new Order { Card = Secret }, DateTimeOffset.UtcNow.AddSeconds(-1), Ct);

        var wire = await DrainAsync(node["Out"]);
        Assert.Equal(3, wire.Count);
        Assert.All(wire, w => Assert.DoesNotContain(Secret, w));
        Assert.All(wire, w => Assert.Contains("\"protection\"", w));
    }

    [Fact]
    public async Task PostRaw_NotAnEnvelope_ToAProtectedEndpoint_Throws()
    {
        var node = CreateNode(Aes("k1", ("k1", 1)));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => node.Broker.PostRawAsync("Out", "<order/>"u8.ToArray(), Ct));
        Assert.Contains("Out", ex.Message);
    }

    // --- Receive ---

    [Fact]
    public async Task Received_ProtectedMessage_IsDecryptedForRouting()
    {
        var keys = Aes("k1", ("k1", 1));
        var wire = await SendProtectedAsync(CreateNode(keys));
        var receiver = CreateNode(keys);

        var result = await receiver.Broker.ProcessAsync(wire, "In", Ct);

        Assert.Equal(ProcessOutcome.Completed, result.Outcome);
        using var forwarded = JsonDocument.Parse(Assert.Single(await DrainAsync(receiver["Plain"])));
        Assert.Equal(Secret, forwarded.RootElement.GetProperty("message").GetProperty("card").GetString());
        Assert.False(forwarded.RootElement.TryGetProperty("protection", out _));
    }

    [Fact]
    public async Task Received_Message_RoutedToAProtectedEndpoint_IsProtectedAgain()
    {
        var keys = Aes("k1", ("k1", 1));
        var hop = CreateNode(keys, routeTo: "Out");

        await hop.Broker.ProcessAsync(Encoding.UTF8.GetBytes(PlaintextEnvelope()), "In", Ct);
        var wire = Assert.Single(await DrainAsync(hop["Out"]));

        Assert.DoesNotContain(Secret, wire);
        var receiver = CreateNode(keys);
        await receiver.Broker.ProcessAsync(Encoding.UTF8.GetBytes(wire), "In", Ct);
        Assert.Contains(Secret, Assert.Single(await DrainAsync(receiver["Plain"])));
    }

    [Fact]
    public async Task Received_WithARotatedKeyRing_DecryptsTheOldKey()
    {
        var wire = await SendProtectedAsync(CreateNode(Aes("2026-10", ("2026-10", 1))));
        var receiver = CreateNode(Aes("2026-11", ("2026-10", 1), ("2026-11", 2)));

        await receiver.Broker.ProcessAsync(wire, "In", Ct);

        Assert.Contains(Secret, Assert.Single(await DrainAsync(receiver["Plain"])));
    }

    [Fact]
    public async Task Received_WithTheWrongKey_IsDeadLettered_AndStaysEncrypted()
    {
        var wire = await SendProtectedAsync(CreateNode(Aes("k1", ("k1", 1))));
        var receiver = CreateNode(Aes("k1", ("k1", 2)));

        var result = await receiver.Broker.ProcessAsync(wire, "In", Ct);

        Assert.Equal(ProcessOutcome.Completed, result.Outcome);   // Memory: the broker dead-letters it itself
        Assert.Empty(await DrainAsync(receiver["Plain"]));
        var dead = Assert.Single(await DrainAsync(receiver["Dlq"]));
        Assert.DoesNotContain(Secret, dead);
        using var envelope = JsonDocument.Parse(dead);
        Assert.Equal(DeadLetterReasons.PayloadUnprotectFailed, envelope.RootElement.GetProperty("deadLetter").GetProperty("reason").GetString());
        Assert.True(envelope.RootElement.TryGetProperty("protection", out _));
    }

    [Fact]
    public async Task Received_ProtectedMessage_WithoutAProtector_IsDeadLettered()
    {
        var wire = await SendProtectedAsync(CreateNode(Aes("k1", ("k1", 1))));
        var receiver = CreateNode(protector: null);

        await receiver.Broker.ProcessAsync(wire, "In", Ct);

        Assert.Equal(DeadLetterReasons.PayloadUnprotectFailed, await DeadLetterReasonAsync(receiver));
    }

    [Fact]
    public async Task Received_WithAChangedEnvelopeId_IsDeadLettered()
    {
        var keys = Aes("k1", ("k1", 1));
        var wire = Encoding.UTF8.GetString(await SendProtectedAsync(CreateNode(keys)));
        using var original = JsonDocument.Parse(wire);
        var id = original.RootElement.GetProperty("id").GetString()!;
        var receiver = CreateNode(keys);

        // The ciphertext is bound to its envelope: moved under another id, it no longer decrypts.
        await receiver.Broker.ProcessAsync(Encoding.UTF8.GetBytes(wire.Replace(id, Guid.NewGuid().ToString())), "In", Ct);

        Assert.Equal(DeadLetterReasons.PayloadUnprotectFailed, await DeadLetterReasonAsync(receiver));
    }

    [Fact]
    public async Task Received_Plaintext_IsAccepted_WhenNotRequired()
    {
        var receiver = CreateNode(Aes("k1", ("k1", 1)));

        await receiver.Broker.ProcessAsync(Encoding.UTF8.GetBytes(PlaintextEnvelope()), "In", Ct);

        Assert.Contains(Secret, Assert.Single(await DrainAsync(receiver["Plain"])));
    }

    [Fact]
    public async Task Received_Plaintext_OnAnEndpointThatRequiresProtection_IsDeadLettered()
    {
        var receiver = CreateNode(Aes("k1", ("k1", 1)), require: ["In"]);

        await receiver.Broker.ProcessAsync(Encoding.UTF8.GetBytes(PlaintextEnvelope()), "In", Ct);

        Assert.Empty(await DrainAsync(receiver["Plain"]));
        Assert.Equal(DeadLetterReasons.PayloadNotProtected, await DeadLetterReasonAsync(receiver));
    }

    [Fact]
    public async Task Received_Protected_OnAnEndpointThatRequiresProtection_IsProcessed()
    {
        var keys = Aes("k1", ("k1", 1));
        var wire = await SendProtectedAsync(CreateNode(keys));
        var receiver = CreateNode(keys, require: ["In"]);

        await receiver.Broker.ProcessAsync(wire, "In", Ct);

        Assert.Contains(Secret, Assert.Single(await DrainAsync(receiver["Plain"])));
    }

    [Fact]
    public async Task SplitAndCompressedMessage_EveryPartIsProtected_AndReassembles()
    {
        var keys = Aes("k1", ("k1", 1));
        var sender = CreateNode(keys);
        // Random hex: compresses only to about half, so it still needs several parts.
        var big = Secret + Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(10_000));

        await sender.Broker.PostAsync("Out", new Order { Id = "big", Card = big }, Ct, splitThresholdBytes: 2048);
        var parts = await DrainAsync(sender["Out"]);

        Assert.True(parts.Count > 1);
        Assert.All(parts, p => Assert.Contains("\"protection\"", p));
        var receiver = CreateNode(keys, require: ["In"]);   // the reassembled message is not checked again
        foreach (var part in parts)
            await receiver.Broker.ProcessAsync(Encoding.UTF8.GetBytes(part), "In", Ct);
        using var forwarded = JsonDocument.Parse(Assert.Single(await DrainAsync(receiver["Plain"])));
        Assert.Equal(big, forwarded.RootElement.GetProperty("message").GetProperty("card").GetString());
    }

    [Fact]
    public async Task WireTap_ToAProtectedEndpoint_ForwardsAProtectedMessageUnchanged()
    {
        var keys = Aes("k1", ("k1", 1));
        var wire = await SendProtectedAsync(CreateNode(keys));
        var receiver = CreateNode(keys, protect: ["Out", "Tap"]);
        receiver.Broker.AddWireTap("Tap");

        await receiver.Broker.ProcessAsync(wire, "In", Ct);

        Assert.Equal(Encoding.UTF8.GetString(wire), Assert.Single(await DrainAsync(receiver["Tap"])));
    }

    [Fact]
    public async Task WireTap_ToAProtectedEndpoint_ProtectsAPlaintextMessage()
    {
        var receiver = CreateNode(Aes("k1", ("k1", 1)), protect: ["Tap"]);
        receiver.Broker.AddWireTap("Tap");

        await receiver.Broker.ProcessAsync(Encoding.UTF8.GetBytes(PlaintextEnvelope()), "In", Ct);

        var tapped = Assert.Single(await DrainAsync(receiver["Tap"]));
        Assert.DoesNotContain(Secret, tapped);
        Assert.Contains("\"protection\"", tapped);
    }

    // --- Builder ---

    [Fact]
    public async Task Builder_ProtectPayloads_ProtectsTheListedEndpoints()
    {
        var services = new ServiceCollection().AddLogging();
        services.AddNymBroker()
            .AddMemoryEndPoint("Out", mode: EndpointMode.WriteOnly)
            .AddMemoryEndPoint("Plain", mode: EndpointMode.WriteOnly)
            .ProtectPayloads(_ => Aes("k1", ("k1", 1)), o => o.ForEndpoints("Out"))
            .Build();
        await using var sp = services.BuildServiceProvider();
        var broker = sp.GetRequiredService<INymBroker>();

        await broker.PostAsync("Out", new Order { Card = Secret }, Ct);
        await broker.PostAsync("Plain", new Order { Card = Secret }, Ct);

        Assert.DoesNotContain(Secret, Assert.Single(await DrainAsync((MemoryQueueEndPoint)sp.GetRequiredKeyedService<IEndPoint>("Out"))));
        Assert.Contains(Secret, Assert.Single(await DrainAsync((MemoryQueueEndPoint)sp.GetRequiredKeyedService<IEndPoint>("Plain"))));
    }

    [Fact]
    public void Builder_UnknownEndpoint_Throws()
    {
        var builder = new ServiceCollection().AddNymBroker()
            .AddMemoryEndPoint("Out")
            .ProtectPayloads(Aes("k1", ("k1", 1)), o => o.ForEndpoints("Out", "Nope").RequireOnReceive("Missing"));

        var ex = Assert.Throws<InvalidOperationException>(builder.Build);
        Assert.Contains("Nope", ex.Message);
        Assert.Contains("Missing", ex.Message);
    }

    private sealed class ToXml : IOutputTransformer
    {
        public byte[]? Transform(RawMessageContext message, string destinationEndpoint) => "<x/>"u8.ToArray();
    }

    private sealed class FromXml : IInputTransformer
    {
        public RawMessageContext? Transform(ReadOnlySpan<byte> raw, string? sourceEndpoint) => null;
    }

    [Fact]
    public void Builder_ProtectedEndpointWithAnOutputTransformer_Throws()
    {
        var builder = new ServiceCollection().AddNymBroker()
            .AddMemoryEndPoint("Out", mode: EndpointMode.WriteOnly)
            .AddOutputTransformer<ToXml>("Out")
            .ProtectPayloads(Aes("k1", ("k1", 1)), o => o.ForEndpoints("Out"));

        Assert.Contains("output transformer", Assert.Throws<InvalidOperationException>(builder.Build).Message);
    }

    [Fact]
    public void Builder_RequireOnReceiveWithAnInputTransformer_Throws()
    {
        var builder = new ServiceCollection().AddNymBroker()
            .AddMemoryEndPoint("In")
            .AddInputTransformer<FromXml>("In")
            .ProtectPayloads(Aes("k1", ("k1", 1)), o => o.RequireOnReceive("In"));

        Assert.Contains("input transformer", Assert.Throws<InvalidOperationException>(builder.Build).Message);
    }

    [Fact]
    public void Options_EmptyName_Throws()
        => Assert.Throws<ArgumentException>(() => new PayloadProtectionOptions().ForEndpoints(" "));
}
