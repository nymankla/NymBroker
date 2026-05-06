using System.Text.Json;
using NymBroker.Core.Message;

namespace NymBroker.Core.Route;

public interface IRouteCondition
{
    bool Evaluate(IMessageContext context, JsonElement messageElement);
}
