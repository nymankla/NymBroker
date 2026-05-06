using System.Text;
using System.Text.Json;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;

namespace NymBroker.Tests;

public sealed class SerializerAdditionalTests
{
    private readonly MessageSerializerJson _sut = new();

    private sealed class Product { public string Name { get; set; } = ""; public decimal Price { get; set; } }

    // --- TraceState round-trip ---

    [Fact]
    public void Serialize_TraceState_IsPreservedRoundTrip()
    {
        var ctx = new MessageContext<Product>
        {
            Message = new Product { Name = "Widget", Price = 9.99m },
            TraceParent = "00-abc-def-01",
            TraceState = "vendor1=value1,vendor2=value2"
        };

        using var stream = _sut.Serialize(ctx);
        var deserialized = _sut.Deserialize(stream);

        Assert.Equal("vendor1=value1,vendor2=value2", deserialized.TraceState);
        Assert.Equal("00-abc-def-01", deserialized.TraceParent);
    }

    // --- Stream overload ---

    [Fact]
    public void Deserialize_Stream_ReturnsCorrectContext()
    {
        var ctx = new MessageContext<Product> { Message = new Product { Name = "Gadget", Price = 19m } };
        using var stream = _sut.Serialize(ctx);

        var result = _sut.Deserialize(stream) as RawMessageContext;

        Assert.NotNull(result);
        Assert.Equal(ctx.Id, result.Id);
        Assert.Equal(ctx.CorrelationId, result.CorrelationId);
    }

    // --- RawMessageContext reuse in Serialize ---

    [Fact]
    public void Serialize_RawMessageContext_ReusesRawElement_WithoutExtraConversion()
    {
        var original = new MessageContext<Product> { Message = new Product { Name = "Gadget", Price = 5m } };
        using var stream1 = _sut.Serialize(original);

        // Deserialize to RawMessageContext, then re-serialize.
        var raw = (RawMessageContext)_sut.Deserialize(stream1);
        using var stream2 = _sut.Serialize(raw);

        // The re-serialized JSON should still contain the product data.
        using var doc = JsonDocument.Parse(stream2);
        Assert.Equal("Gadget", doc.RootElement.GetProperty("message").GetProperty("name").GetString());
    }

    // --- Null / invalid JSON ---

    [Fact]
    public void Deserialize_InvalidJson_ThrowsException()
    {
        Assert.ThrowsAny<Exception>(() => _sut.Deserialize("not json at all"));
    }

    [Fact]
    public void Deserialize_NullPayloadJson_ThrowsException()
    {
        // JSON "null" deserializes MessageContextDto as null → InvalidOperationException.
        Assert.Throws<InvalidOperationException>(() => _sut.Deserialize("null"));
    }

    // --- DeserializeMessage helpers ---

    [Fact]
    public void DeserializeMessage_ReturnsTypedPayload()
    {
        var ctx = new MessageContext<Product> { Message = new Product { Name = "Item", Price = 3m } };
        using var stream = _sut.Serialize(ctx);
        var raw = (RawMessageContext)_sut.Deserialize(stream);

        var product = MessageSerializerJson.DeserializeMessage<Product>(raw);

        Assert.NotNull(product);
        Assert.Equal("Item", product.Name);
        Assert.Equal(3m, product.Price);
    }

    [Fact]
    public void DeserializeMessageObject_ReturnsBoxedInstance()
    {
        var ctx = new MessageContext<Product> { Message = new Product { Name = "Box", Price = 1m } };
        using var stream = _sut.Serialize(ctx);
        var raw = (RawMessageContext)_sut.Deserialize(stream);

        var obj = MessageSerializerJson.DeserializeMessageObject(raw, typeof(Product));

        Assert.NotNull(obj);
        Assert.IsType<Product>(obj);
        Assert.Equal("Box", ((Product)obj).Name);
    }

    // --- CorrelationId and Created preservation ---

    [Fact]
    public void Serialize_CorrelationId_IsPreserved()
    {
        var corrId = Guid.NewGuid();
        var ctx = new MessageContext<Product>
        {
            Message = new Product { Name = "X" },
            CorrelationId = corrId
        };
        using var stream = _sut.Serialize(ctx);
        var result = _sut.Deserialize(stream);
        Assert.Equal(corrId, result.CorrelationId);
    }

    [Fact]
    public void Serialize_Created_IsPreservedToSecondPrecision()
    {
        var created = new DateTime(2025, 6, 1, 12, 30, 0, DateTimeKind.Utc);
        var ctx = new MessageContext<Product>
        {
            Message = new Product { Name = "Y" },
            Created = created
        };
        using var stream = _sut.Serialize(ctx);
        var result = _sut.Deserialize(stream);
        Assert.Equal(created, result.Created.ToUniversalTime());
    }
}
