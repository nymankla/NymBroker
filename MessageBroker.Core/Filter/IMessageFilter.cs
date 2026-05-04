using MessageBroker.Core.Message;

namespace MessageBroker.Core.Filter;

public interface IMessageFilter
{
    /// <summary>Returns a (possibly modified) context, or null to discard the message.</summary>
    IMessageContext? Filter(IMessageContext context);
}
