using System.Text.Json;

namespace NymBroker.Core.Route;

public interface IRouteBuilder
{
    RouteContext Build();
}

public interface IRouteBuilder<T> : IRouteBuilder where T : class
{
    IRouteBuilder<T> To(string endpointName);
    IRouteBuilder<T> When(Func<JsonElement, bool> condition);
    IRouteBuilder<T> WhenFrom(string sourceEndpoint);
    IRouteBuilder<T> WhenNotFrom(string sourceEndpoint);
    IRouteBuilder<T> WhenMessageIsOlderThan(TimeSpan age);
    IRouteBuilder<T> And(IRouteCondition lhs, IRouteCondition rhs);
    IRouteBuilder<T> Or(IRouteCondition lhs, IRouteCondition rhs);
    IRouteBuilder<T> Transform(string fileName);
    new RouteContext Build();
}
