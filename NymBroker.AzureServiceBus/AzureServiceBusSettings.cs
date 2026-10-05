using System.Text.Json.Serialization;
using Azure.Core;

namespace NymBroker.AzureServiceBus;

public sealed class AzureServiceBusSettings
{
    /// <summary>Connection string (SAS or emulator). Use this <b>or</b> <see cref="FullyQualifiedNamespace"/> + <see cref="Credential"/>.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>e.g. <c>myns.servicebus.windows.net</c>; used together with <see cref="Credential"/>.</summary>
    public string? FullyQualifiedNamespace { get; set; }

    /// <summary>Azure AD credential (e.g. <c>DefaultAzureCredential</c> from Azure.Identity). Code-only — not read from config files.</summary>
    [JsonIgnore]
    public TokenCredential? Credential { get; set; }

    /// <summary>Queue to send to and receive from. Use this <b>or</b> <see cref="TopicName"/>.</summary>
    public string? QueueName { get; set; }

    /// <summary>Topic to send to; receiving needs <see cref="SubscriptionName"/> as well.</summary>
    public string? TopicName { get; set; }

    public string? SubscriptionName { get; set; }

    /// <summary>Receive from the entity's dead-letter sub-queue instead of the entity itself (for repair / replay endpoints).</summary>
    public bool ReadDeadLetterQueue { get; set; }

    /// <summary>Messages handled in parallel. Values above 1 give up ordering.</summary>
    public int MaxConcurrentCalls { get; set; } = 1;

    public int PrefetchCount { get; set; }

    /// <summary>How long the processor keeps renewing a message lock while the handler runs.</summary>
    public TimeSpan MaxAutoLockRenewalDuration { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// When true (default), failures are settled by Service Bus: consumer failures are abandoned and redelivered until the
    /// entity's <c>MaxDeliveryCount</c> moves them to the dead-letter queue; messages that can never succeed are
    /// dead-lettered at once with the reason. When false, the broker posts failures to its own dead-letter endpoint and
    /// the message is completed.
    /// </summary>
    public bool UseNativeDeadLetter { get; set; } = true;

    /// <summary>The entity <see cref="Azure.Messaging.ServiceBus.ServiceBusSender"/> sends to.</summary>
    internal string SendEntity => QueueName ?? TopicName!;

    /// <summary>Throws <see cref="InvalidOperationException"/> describing the first invalid combination.</summary>
    internal void Validate(string endpointName)
    {
        var hasConnectionString = !string.IsNullOrWhiteSpace(ConnectionString);
        var hasNamespace = !string.IsNullOrWhiteSpace(FullyQualifiedNamespace);

        if (hasConnectionString == hasNamespace)
            throw new InvalidOperationException(
                $"Azure Service Bus endpoint '{endpointName}': set either ConnectionString or FullyQualifiedNamespace (with Credential), not both or neither.");
        if (hasNamespace && Credential is null)
            throw new InvalidOperationException(
                $"Azure Service Bus endpoint '{endpointName}': FullyQualifiedNamespace requires a Credential.");

        var hasQueue = !string.IsNullOrWhiteSpace(QueueName);
        var hasTopic = !string.IsNullOrWhiteSpace(TopicName);
        if (hasQueue == hasTopic)
            throw new InvalidOperationException(
                $"Azure Service Bus endpoint '{endpointName}': set either QueueName or TopicName, not both or neither.");
        if (hasQueue && !string.IsNullOrWhiteSpace(SubscriptionName))
            throw new InvalidOperationException(
                $"Azure Service Bus endpoint '{endpointName}': SubscriptionName only applies to topics.");

        if (MaxConcurrentCalls < 1)
            throw new InvalidOperationException($"Azure Service Bus endpoint '{endpointName}': MaxConcurrentCalls must be at least 1.");
        if (PrefetchCount < 0)
            throw new InvalidOperationException($"Azure Service Bus endpoint '{endpointName}': PrefetchCount cannot be negative.");
    }
}
