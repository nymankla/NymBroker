using System.Collections.Immutable;
using System.Threading;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Message;

namespace NymBroker.Core.Impl;

public sealed class MessageTypeRegistry
{
    private ImmutableDictionary<string, Type> _messageTypes = ImmutableDictionary<string, Type>.Empty;

    public MessageTypeRegistry()
    {
        Register(typeof(SplitMessage));
    }

    public void Register(Type messageType)
    {
        ImmutableInterlocked.Update(ref _messageTypes, dict =>
        {
            dict = dict.SetItem(MessageTypeName.Get(messageType), messageType);
            if (!string.IsNullOrWhiteSpace(messageType.FullName))
                dict = dict.SetItem(messageType.FullName, messageType);
            if (!string.IsNullOrWhiteSpace(messageType.AssemblyQualifiedName))
                dict = dict.SetItem(messageType.AssemblyQualifiedName, messageType);
            return dict;
        });
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
