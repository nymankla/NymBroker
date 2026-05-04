using System.Collections.Concurrent;
using System.Reflection;

namespace MessageBroker.Core.Message;

[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class MessageNameAttribute(string name) : Attribute
{
    public string Name { get; } = string.IsNullOrWhiteSpace(name)
        ? throw new ArgumentException("Message name cannot be empty.", nameof(name))
        : name;
}

public static class MessageTypeName
{
    private static readonly ConcurrentDictionary<Type, string> Cache = new();

    public static string Get(Type type)
        => Cache.GetOrAdd(type, static t =>
            t.GetCustomAttribute<MessageNameAttribute>()?.Name
            ?? t.FullName
            ?? t.Name);
}
