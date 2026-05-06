using NymBroker.Core.Aggregator;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;

namespace NymBroker.Tests;

public sealed class MessageTypeRegistryTests
{
    [MessageName("orders.created")]
    private sealed class NamedOrder { }

    private sealed class PlainOrder { }

    [Fact]
    public void SplitMessage_IsPreRegistered()
    {
        var registry = new MessageTypeRegistry();
        Assert.Equal(typeof(SplitMessage), registry.Resolve(MessageTypeName.Get(typeof(SplitMessage))));
    }

    [Fact]
    public void Register_And_Resolve_ByMessageNameAttribute()
    {
        var registry = new MessageTypeRegistry();
        registry.Register(typeof(NamedOrder));
        Assert.Equal(typeof(NamedOrder), registry.Resolve("orders.created"));
    }

    [Fact]
    public void Register_And_Resolve_ByFullName()
    {
        var registry = new MessageTypeRegistry();
        registry.Register(typeof(PlainOrder));
        Assert.Equal(typeof(PlainOrder), registry.Resolve(typeof(PlainOrder).FullName));
    }

    [Fact]
    public void Register_And_Resolve_ByAssemblyQualifiedName()
    {
        var registry = new MessageTypeRegistry();
        registry.Register(typeof(PlainOrder));
        Assert.Equal(typeof(PlainOrder), registry.Resolve(typeof(PlainOrder).AssemblyQualifiedName));
    }

    [Fact]
    public void Resolve_UnknownName_ReturnsNull()
    {
        var registry = new MessageTypeRegistry();
        Assert.Null(registry.Resolve("completely.unknown.type.name"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Resolve_NullOrWhitespace_ReturnsNull(string? typeName)
    {
        var registry = new MessageTypeRegistry();
        Assert.Null(registry.Resolve(typeName));
    }

    [Fact]
    public void Resolve_FallsBackToTypeGetType_ForAssemblyQualifiedName_WithoutExplicitRegistration()
    {
        var registry = new MessageTypeRegistry();
        // PlainOrder is in this assembly; Type.GetType should resolve it by assembly-qualified name.
        var result = registry.Resolve(typeof(PlainOrder).AssemblyQualifiedName);
        Assert.Equal(typeof(PlainOrder), result);
    }

    [Fact]
    public void Register_OverwritesPreviousEntry_WithSameName()
    {
        var registry = new MessageTypeRegistry();
        registry.Register(typeof(PlainOrder));
        // Register again — should not throw and should still resolve correctly.
        registry.Register(typeof(PlainOrder));
        Assert.Equal(typeof(PlainOrder), registry.Resolve(typeof(PlainOrder).FullName));
    }
}
