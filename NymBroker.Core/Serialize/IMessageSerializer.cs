using NymBroker.Core.Message;

namespace NymBroker.Core.Serialize;

public interface IMessageSerializer
{
    Stream Serialize(IMessageContext context);
    IMessageContext Deserialize(string json);
    IMessageContext Deserialize(Stream stream);
    IMessageContext Deserialize(ReadOnlySpan<byte> utf8Json);
}
