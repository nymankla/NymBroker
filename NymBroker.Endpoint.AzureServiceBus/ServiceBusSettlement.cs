using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using NymBroker.Core.Endpoint;

namespace NymBroker.Endpoint.AzureServiceBus;

/// <summary>The three ways to settle a peek-locked message. Abstracted so the mapping below is unit-testable.</summary>
internal interface IServiceBusMessageSettler
{
    Task CompleteAsync(CancellationToken ct);
    Task AbandonAsync(CancellationToken ct);
    Task DeadLetterAsync(string reason, string? description, CancellationToken ct);
}

/// <summary>Settles through the processor's event args (which own the message lock).</summary>
internal sealed class ProcessMessageEventArgsSettler(ProcessMessageEventArgs args) : IServiceBusMessageSettler
{
    public Task CompleteAsync(CancellationToken ct) => args.CompleteMessageAsync(args.Message, ct);
    public Task AbandonAsync(CancellationToken ct) => args.AbandonMessageAsync(args.Message, cancellationToken: ct);
    public Task DeadLetterAsync(string reason, string? description, CancellationToken ct)
        => args.DeadLetterMessageAsync(args.Message, reason, description, ct);
}

/// <summary>Maps a <see cref="ProcessResult"/> to a Service Bus settlement.</summary>
internal static class ServiceBusSettlement
{
    /// <summary>Service Bus limits the dead-letter reason and description properties to 4 KB each.</summary>
    internal const int MaxDeadLetterPropertyLength = 4096;

    internal static async Task SettleAsync(
        ProcessResult result,
        bool readingDeadLetterQueue,
        IServiceBusMessageSettler settler,
        ILogger logger,
        string endpointName,
        string? messageId,
        int deliveryCount,
        CancellationToken ct)
    {
        switch (result.Outcome)
        {
            case ProcessOutcome.Completed:
                await settler.CompleteAsync(ct);
                break;

            case ProcessOutcome.DeadLetter when readingDeadLetterQueue:
                // Already in the dead-letter queue: dead-lettering again isn't possible, so remove it and say so.
                logger.LogWarning("Message {MessageId} read from the dead-letter queue of endpoint '{Name}' still cannot be processed and is removed: {Failure}",
                    messageId, endpointName, result.FailureText);
                await settler.CompleteAsync(ct);
                break;

            case ProcessOutcome.DeadLetter:
                logger.LogWarning("Message {MessageId} on endpoint '{Name}' dead-lettered: {Failure}", messageId, endpointName, result.FailureText);
                await settler.DeadLetterAsync(
                    Truncate(result.Reason ?? DeadLetterReasons.ConsumerFailed)!,
                    Truncate(result.Description),
                    ct);
                break;

            default:
                // Retry: abandon so Service Bus redelivers it; the entity's MaxDeliveryCount dead-letters it
                // (reason "MaxDeliveryCountExceeded") once it keeps failing.
                logger.LogWarning("Message {MessageId} on endpoint '{Name}' abandoned for redelivery (delivery {DeliveryCount}): {Failure}",
                    messageId, endpointName, deliveryCount, result.FailureText);
                await settler.AbandonAsync(ct);
                break;
        }
    }

    internal static string? Truncate(string? value)
        => value is { Length: > MaxDeadLetterPropertyLength } ? value[..MaxDeadLetterPropertyLength] : value;
}
