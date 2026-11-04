using System.Linq.Expressions;
using NymBroker.Core.Message;

namespace NymBroker.Core.Impl;

/// <summary>Compiles the call of a consumer's or subscriber's typed method, so the broker need not use reflection per message.</summary>
internal static class DispatchDelegate
{
    public static Func<TTarget, object, IMessageContext, CancellationToken, Task> Compile<TTarget>(Type targetInterface, string methodName)
    {
        var target = Expression.Parameter(typeof(TTarget), "target");
        var message = Expression.Parameter(typeof(object), "message");
        var context = Expression.Parameter(typeof(IMessageContext), "context");
        var ct = Expression.Parameter(typeof(CancellationToken), "ct");

        var call = Expression.Call(
            Expression.Convert(target, targetInterface),
            targetInterface.GetMethod(methodName)!,
            Expression.Convert(message, targetInterface.GetGenericArguments()[0]),
            context,
            ct);

        return Expression.Lambda<Func<TTarget, object, IMessageContext, CancellationToken, Task>>(call, target, message, context, ct).Compile();
    }
}
