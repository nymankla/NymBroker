namespace MessageBroker.Core.Impl;

internal readonly struct MessageKey : IEquatable<MessageKey>
{
    private readonly Type _type;

    public MessageKey(Type type) => _type = type;

    public bool Equals(MessageKey other) => _type == other._type;
    public override bool Equals(object? obj) => obj is MessageKey k && Equals(k);
    public override int GetHashCode() => _type.GetHashCode();
    public override string ToString() => _type.Name;
}
