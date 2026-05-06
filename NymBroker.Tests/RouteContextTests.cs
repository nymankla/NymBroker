using System.Text.Json;
using NymBroker.Core.Message;
using NymBroker.Core.Route;

namespace NymBroker.Tests;

public sealed class RouteContextTests
{
    private sealed class MessageA { }
    private sealed class MessageB { }

    private static readonly JsonElement EmptyElement = default;

    private static IMessageContext ContextFrom(string? source) =>
        new MessageContext<object> { Address = EndpointAddress.Create("dest", source) };

    // --- MessageType matching ---

    [Fact]
    public void Evaluate_IAnyMessage_MatchesAnyMessageType()
    {
        var ctx = new RouteContext { MessageType = typeof(IAnyMessage), DestinationEndpoint = "D" };
        Assert.True(ctx.Evaluate(typeof(MessageA), ContextFrom(null), EmptyElement));
        Assert.True(ctx.Evaluate(typeof(MessageB), ContextFrom(null), EmptyElement));
    }

    [Fact]
    public void Evaluate_SpecificType_MatchesSameType()
    {
        var ctx = new RouteContext { MessageType = typeof(MessageA), DestinationEndpoint = "D" };
        Assert.True(ctx.Evaluate(typeof(MessageA), ContextFrom(null), EmptyElement));
    }

    [Fact]
    public void Evaluate_SpecificType_DoesNotMatchDifferentType()
    {
        var ctx = new RouteContext { MessageType = typeof(MessageA), DestinationEndpoint = "D" };
        Assert.False(ctx.Evaluate(typeof(MessageB), ContextFrom(null), EmptyElement));
    }

    // --- SourceEndpoint ---

    [Fact]
    public void Evaluate_WithSourceEndpoint_ReturnsTrue_WhenSourceMatches()
    {
        var ctx = new RouteContext { MessageType = typeof(IAnyMessage), SourceEndpoint = "A", DestinationEndpoint = "D" };
        Assert.True(ctx.Evaluate(typeof(MessageA), ContextFrom("A"), EmptyElement));
    }

    [Fact]
    public void Evaluate_WithSourceEndpoint_ReturnsFalse_WhenSourceDoesNotMatch()
    {
        var ctx = new RouteContext { MessageType = typeof(IAnyMessage), SourceEndpoint = "A", DestinationEndpoint = "D" };
        Assert.False(ctx.Evaluate(typeof(MessageA), ContextFrom("B"), EmptyElement));
    }

    [Fact]
    public void Evaluate_WithSourceEndpoint_IsCaseInsensitive()
    {
        var ctx = new RouteContext { MessageType = typeof(IAnyMessage), SourceEndpoint = "alpha", DestinationEndpoint = "D" };
        Assert.True(ctx.Evaluate(typeof(MessageA), ContextFrom("ALPHA"), EmptyElement));
    }

    // --- ExcludedSourceEndpoint ---

    [Fact]
    public void Evaluate_WithExcludedSourceEndpoint_ReturnsFalse_WhenSourceMatches()
    {
        var ctx = new RouteContext { MessageType = typeof(IAnyMessage), ExcludedSourceEndpoint = "Blocked", DestinationEndpoint = "D" };
        Assert.False(ctx.Evaluate(typeof(MessageA), ContextFrom("Blocked"), EmptyElement));
    }

    [Fact]
    public void Evaluate_WithExcludedSourceEndpoint_ReturnsTrue_WhenSourceDiffers()
    {
        var ctx = new RouteContext { MessageType = typeof(IAnyMessage), ExcludedSourceEndpoint = "Blocked", DestinationEndpoint = "D" };
        Assert.True(ctx.Evaluate(typeof(MessageA), ContextFrom("Allowed"), EmptyElement));
    }

    [Fact]
    public void Evaluate_WithExcludedSourceEndpoint_IsCaseInsensitive()
    {
        var ctx = new RouteContext { MessageType = typeof(IAnyMessage), ExcludedSourceEndpoint = "blocked", DestinationEndpoint = "D" };
        Assert.False(ctx.Evaluate(typeof(MessageA), ContextFrom("BLOCKED"), EmptyElement));
    }

    // --- Condition ---

    [Fact]
    public void Evaluate_WithCondition_ReturnsTrue_WhenConditionPasses()
    {
        var ctx = new RouteContext
        {
            MessageType = typeof(IAnyMessage),
            DestinationEndpoint = "D",
            Condition = new JsonRouteCondition(_ => true)
        };
        Assert.True(ctx.Evaluate(typeof(MessageA), ContextFrom(null), EmptyElement));
    }

    [Fact]
    public void Evaluate_WithCondition_ReturnsFalse_WhenConditionFails()
    {
        var ctx = new RouteContext
        {
            MessageType = typeof(IAnyMessage),
            DestinationEndpoint = "D",
            Condition = new JsonRouteCondition(_ => false)
        };
        Assert.False(ctx.Evaluate(typeof(MessageA), ContextFrom(null), EmptyElement));
    }

    // --- No restrictions means always match ---

    [Fact]
    public void Evaluate_WithNoRestrictions_AlwaysReturnsTrue()
    {
        var ctx = new RouteContext { MessageType = typeof(IAnyMessage), DestinationEndpoint = "D" };
        Assert.True(ctx.Evaluate(typeof(MessageA), ContextFrom("SomeSource"), EmptyElement));
    }
}
