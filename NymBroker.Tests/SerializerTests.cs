using System.Text;
using System.Text.Json;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;

namespace NymBroker.Tests;

public sealed class SerializerTests
{
    private readonly MessageSerializerJson _sut = new();

    private sealed class Order { public string Id { get; set; } = "X"; public decimal Amount { get; set; } }

    [MessageName("orders.created")]
    private sealed class NamedOrder { public string Id { get; set; } = "X"; }

    [Fact]
    public void Serialize_ThenDeserialize_PreservesEnvelopeFields()
    {
        var ctx = new MessageContext<Order>
        {
            Message = new Order { Id = "O1", Amount = 100m },
            Address = EndpointAddress.Create("dest", "src")
        };

        using var stream = _sut.Serialize(ctx);
        var deserialized = _sut.Deserialize(stream);

        Assert.Equal(ctx.Id, deserialized.Id);
        Assert.Equal(ctx.CorrelationId, deserialized.CorrelationId);
        Assert.Equal("dest", deserialized.Address?.To);
        Assert.Equal("src", deserialized.Address?.From);
        Assert.Contains("Order", deserialized.MessageType);
    }

    [Fact]
    public void Serialize_ProducesValidJson()
    {
        var ctx = new MessageContext<Order> { Message = new Order { Id = "O2", Amount = 50m } };
        using var stream = _sut.Serialize(ctx);
        using var doc = JsonDocument.Parse(stream);
        Assert.Equal("O2", doc.RootElement.GetProperty("message").GetProperty("id").GetString());
    }

    [Fact]
    public void Deserialize_RawMessage_CanBeReadAsTypedObject()
    {
        var ctx = new MessageContext<Order> { Message = new Order { Id = "O3", Amount = 75m } };
        using var stream = _sut.Serialize(ctx);
        var raw = Encoding.UTF8.GetString(((MemoryStream)stream).ToArray());
        var deserialized = (RawMessageContext)_sut.Deserialize(raw);
        var order = MessageSerializerJson.DeserializeMessage<Order>(deserialized);

        Assert.NotNull(order);
        Assert.Equal("O3", order.Id);
        Assert.Equal(75m, order.Amount);
    }

    [Fact]
    public void Serialize_UsesExplicitMessageName_WhenConfigured()
    {
        var ctx = new MessageContext<NamedOrder> { Message = new NamedOrder { Id = "O4" } };
        using var stream = _sut.Serialize(ctx);
        using var doc = JsonDocument.Parse(stream);

        Assert.Equal("orders.created", doc.RootElement.GetProperty("messageType").GetString());
    }
}
