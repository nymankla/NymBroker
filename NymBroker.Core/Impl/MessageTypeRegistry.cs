using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using NymBroker.Core.Aggregator;
using NymBroker.Core.Message;

namespace NymBroker.Core.Impl;

public sealed class MessageTypeRegistry
{
    private ImmutableDictionary<string, Type> _messageTypes = ImmutableDictionary<string, Type>.Empty;

    public MessageTypeRegistry()
    {
        Register(typeof(SplitMessage));
        Register(typeof(UndecodableMessage));
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

        if (!RuntimeFeature.IsDynamicCodeSupported)
            return null;

        resolved = ResolveByReflection(messageType);
        if (resolved != null)
            Register(resolved);

        return resolved;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2057", Justification = "This legacy fallback runs only when dynamic code is supported; Native AOT callers register message types explicitly.")]
    private static Type? ResolveByReflection(string messageType)
        => Type.GetType(messageType, throwOnError: false);
}
