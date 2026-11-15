using System.Text;
using System.Text.Json;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;

namespace NymBroker.Tests;

/// <summary>The serializer reuses one JSON writer and buffer per thread; an envelope must never see an earlier one's bytes.</summary>
public sealed class EnvelopeWriterReuseTests
{
    private sealed class Note { public string Text { get; set; } = string.Empty; }

    private sealed class Faulty
    {
        public string Before => "written before the failure";
        public string Fails => throw new InvalidOperationException("getter failed");
    }

    /// <summary>Serializes another envelope from inside a property getter, while the outer envelope is being written.</summary>
    private sealed class Nested
    {
        public string Inner => Encoding.UTF8.GetString(Serialize(new Note { Text = "inner" }));
    }

    private static byte[] Serialize<T>(T message) where T : class
        => MessageSerializerJson.SerializeToBytes(new MessageContext<T> { Message = message });

    private static string TextOf(byte[] envelope)
    {
        using var document = JsonDocument.Parse(envelope);   // throws on leftover or missing bytes
        return document.RootElement.GetProperty("message").GetProperty("text").GetString()!;
    }

    [Fact]
    public void ASmallEnvelopeAfterALargeOne_HasOnlyItsOwnBytes()
    {
        var large = new string('x', 600_000);   // over the buffer size a thread keeps

        Assert.Equal(large, TextOf(Serialize(new Note { Text = large })));
        Assert.Equal("small", TextOf(Serialize(new Note { Text = "small" })));
        Assert.Equal("again", TextOf(Serialize(new Note { Text = "again" })));
    }

    [Fact]
    public void AnEnvelopeAfterAFailedOne_HasOnlyItsOwnBytes()
    {
        Assert.Throws<InvalidOperationException>(() => Serialize(new Faulty()));

        Assert.Equal("after", TextOf(Serialize(new Note { Text = "after" })));
    }

    [Fact]
    public void AnEnvelopeSerializedWhileAnotherIsWritten_DoesNotDisturbIt()
    {
        var outer = Serialize(new Nested());

        using var document = JsonDocument.Parse(outer);
        var inner = document.RootElement.GetProperty("message").GetProperty("inner").GetString()!;
        Assert.Equal("inner", TextOf(Encoding.UTF8.GetBytes(inner)));
    }
}
