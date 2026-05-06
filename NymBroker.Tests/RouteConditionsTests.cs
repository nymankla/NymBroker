using System.Text.Json;
using NymBroker.Core.Message;
using NymBroker.Core.Route;

namespace NymBroker.Tests;

public sealed class RouteConditionsTests
{
    private static readonly JsonElement EmptyElement = default;

    private static IMessageContext EmptyContext =>
        new MessageContext<object> { Address = null };

    private static IMessageContext ContextFrom(string? source) =>
        new MessageContext<object> { Address = EndpointAddress.Create("dest", source) };

    private static IMessageContext ContextWithCreated(DateTime created) =>
        new MessageContext<object> { Address = null, Created = created };

    // --- JsonRouteCondition ---

    [Fact]
    public void JsonRouteCondition_ReturnsTrue_WhenPredicateIsTrue()
    {
        var condition = new JsonRouteCondition(_ => true);
        Assert.True(condition.Evaluate(EmptyContext, EmptyElement));
    }

    [Fact]
    public void JsonRouteCondition_ReturnsFalse_WhenPredicateIsFalse()
    {
        var condition = new JsonRouteCondition(_ => false);
        Assert.False(condition.Evaluate(EmptyContext, EmptyElement));
    }

    [Fact]
    public void JsonRouteCondition_PassesElementToPredicate()
    {
        using var doc = JsonDocument.Parse("{\"value\":42}");
        var element = doc.RootElement;
        JsonElement captured = default;

        var condition = new JsonRouteCondition(el => { captured = el; return true; });
        condition.Evaluate(EmptyContext, element);

        Assert.Equal(42, captured.GetProperty("value").GetInt32());
    }

    // --- FromRouteCondition ---

    [Fact]
    public void FromRouteCondition_ReturnsTrue_WhenSourceMatches()
    {
        var condition = new FromRouteCondition("Alpha");
        Assert.True(condition.Evaluate(ContextFrom("Alpha"), EmptyElement));
    }

    [Fact]
    public void FromRouteCondition_IsCaseInsensitive()
    {
        var condition = new FromRouteCondition("alpha");
        Assert.True(condition.Evaluate(ContextFrom("ALPHA"), EmptyElement));
    }

    [Fact]
    public void FromRouteCondition_ReturnsFalse_WhenNullAddress()
    {
        var condition = new FromRouteCondition("Alpha");
        Assert.False(condition.Evaluate(EmptyContext, EmptyElement));
    }

    [Fact]
    public void FromRouteCondition_ReturnsFalse_WhenSourceDiffers()
    {
        var condition = new FromRouteCondition("Alpha");
        Assert.False(condition.Evaluate(ContextFrom("Beta"), EmptyElement));
    }

    // --- NotFromRouteCondition ---

    [Fact]
    public void NotFromRouteCondition_ReturnsTrue_WhenSourceDiffers()
    {
        var condition = new NotFromRouteCondition("Blocked");
        Assert.True(condition.Evaluate(ContextFrom("Allowed"), EmptyElement));
    }

    [Fact]
    public void NotFromRouteCondition_IsCaseInsensitive()
    {
        var condition = new NotFromRouteCondition("blocked");
        Assert.False(condition.Evaluate(ContextFrom("BLOCKED"), EmptyElement));
    }

    [Fact]
    public void NotFromRouteCondition_ReturnsTrue_WhenAddressIsNull()
    {
        var condition = new NotFromRouteCondition("Blocked");
        Assert.True(condition.Evaluate(EmptyContext, EmptyElement));
    }

    [Fact]
    public void NotFromRouteCondition_ReturnsFalse_WhenSourceMatches()
    {
        var condition = new NotFromRouteCondition("Blocked");
        Assert.False(condition.Evaluate(ContextFrom("Blocked"), EmptyElement));
    }

    // --- MessageAgeRouteCondition ---

    [Fact]
    public void MessageAgeRouteCondition_ReturnsTrue_WhenMessageIsOlderThanThreshold()
    {
        var condition = new MessageAgeRouteCondition(TimeSpan.FromMinutes(5));
        var ctx = ContextWithCreated(DateTime.UtcNow.AddMinutes(-10));
        Assert.True(condition.Evaluate(ctx, EmptyElement));
    }

    [Fact]
    public void MessageAgeRouteCondition_ReturnsFalse_WhenMessageIsRecent()
    {
        var condition = new MessageAgeRouteCondition(TimeSpan.FromMinutes(5));
        var ctx = ContextWithCreated(DateTime.UtcNow.AddMinutes(-1));
        Assert.False(condition.Evaluate(ctx, EmptyElement));
    }

    // --- AndRouteCondition ---

    [Fact]
    public void AndRouteCondition_ReturnsTrue_WhenBothConditionsAreTrue()
    {
        var condition = new AndRouteCondition(
            new JsonRouteCondition(_ => true),
            new JsonRouteCondition(_ => true));
        Assert.True(condition.Evaluate(EmptyContext, EmptyElement));
    }

    [Fact]
    public void AndRouteCondition_ReturnsFalse_WhenFirstConditionIsFalse()
    {
        var condition = new AndRouteCondition(
            new JsonRouteCondition(_ => false),
            new JsonRouteCondition(_ => true));
        Assert.False(condition.Evaluate(EmptyContext, EmptyElement));
    }

    [Fact]
    public void AndRouteCondition_ReturnsFalse_WhenSecondConditionIsFalse()
    {
        var condition = new AndRouteCondition(
            new JsonRouteCondition(_ => true),
            new JsonRouteCondition(_ => false));
        Assert.False(condition.Evaluate(EmptyContext, EmptyElement));
    }

    [Fact]
    public void AndRouteCondition_ReturnsFalse_WhenBothConditionsAreFalse()
    {
        var condition = new AndRouteCondition(
            new JsonRouteCondition(_ => false),
            new JsonRouteCondition(_ => false));
        Assert.False(condition.Evaluate(EmptyContext, EmptyElement));
    }

    // --- OrRouteCondition ---

    [Fact]
    public void OrRouteCondition_ReturnsTrue_WhenFirstConditionIsTrue()
    {
        var condition = new OrRouteCondition(
            new JsonRouteCondition(_ => true),
            new JsonRouteCondition(_ => false));
        Assert.True(condition.Evaluate(EmptyContext, EmptyElement));
    }

    [Fact]
    public void OrRouteCondition_ReturnsTrue_WhenSecondConditionIsTrue()
    {
        var condition = new OrRouteCondition(
            new JsonRouteCondition(_ => false),
            new JsonRouteCondition(_ => true));
        Assert.True(condition.Evaluate(EmptyContext, EmptyElement));
    }

    [Fact]
    public void OrRouteCondition_ReturnsTrue_WhenBothConditionsAreTrue()
    {
        var condition = new OrRouteCondition(
            new JsonRouteCondition(_ => true),
            new JsonRouteCondition(_ => true));
        Assert.True(condition.Evaluate(EmptyContext, EmptyElement));
    }

    [Fact]
    public void OrRouteCondition_ReturnsFalse_WhenBothConditionsAreFalse()
    {
        var condition = new OrRouteCondition(
            new JsonRouteCondition(_ => false),
            new JsonRouteCondition(_ => false));
        Assert.False(condition.Evaluate(EmptyContext, EmptyElement));
    }
}
