using MessageBroker.Core.Message;

namespace MessageBroker.Core.Serialize;

public interface IMessageSerializer
{
    Stream Serialize(IMessageContext context);
    IMessageContext Deserialize(string json);
    IMessageContext Deserialize(Stream stream);
}
