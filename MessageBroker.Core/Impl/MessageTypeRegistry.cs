using System.Collections.Immutable;
using MessageBroker.Core.Aggregator;
using MessageBroker.Core.Message;

namespace MessageBroker.Core.Impl;

public sealed class MessageTypeRegistry
{
    private ImmutableDictionary<string, Type> _messageTypes = ImmutableDictionary<string, Type>.Empty;

    public MessageTypeRegistry()
    {
        Register(typeof(SplitMessage));
    }

    public void Register(Type messageType)
    {
        _messageTypes = _messageTypes.SetItem(MessageTypeName.Get(messageType), messageType);

        if (!string.IsNullOrWhiteSpace(messageType.FullName))
            _messageTypes = _messageTypes.SetItem(messageType.FullName, messageType);

        if (!string.IsNullOrWhiteSpace(messageType.AssemblyQualifiedName))
            _messageTypes = _messageTypes.SetItem(messageType.AssemblyQualifiedName, messageType);
    }

    public Type? Resolve(string? messageType)
    {
        if (string.IsNullOrWhiteSpace(messageType))
            return null;

        if (_messageTypes.TryGetValue(messageType, out var resolved))
            return resolved;

        resolved = Type.GetType(messageType, throwOnError: false);
        if (resolved != null)
            Register(resolved);

        return resolved;
    }
}
