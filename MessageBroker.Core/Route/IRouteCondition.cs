using System.Text.Json;
using MessageBroker.Core.Message;

namespace MessageBroker.Core.Route;

public interface IRouteCondition
{
    bool Evaluate(IMessageContext context, JsonElement messageElement);
}
