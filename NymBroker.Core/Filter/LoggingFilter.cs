using Microsoft.Extensions.Logging;
using NymBroker.Core.Message;
using NymBroker.Core.Serialize;

namespace NymBroker.Core.Filter;

public sealed class LoggingFilter(ILogger<LoggingFilter> logger) : IMessageFilter
{
    public IMessageContext? Filter(IMessageContext context)
    {
        if (!logger.IsEnabled(LogLevel.Debug)) return context;

        var payload = context is RawMessageContext raw
            ? raw.RawMessage.ToString()
            : null;

        logger.LogDebug(
            "Message received — Id={Id} Type={MessageType} From={From} Created={Created:O} Payload={Payload}",
            context.Id,
            context.MessageType,
            context.Address?.From,
            context.Created,
            payload);

        return context;
    }
}
