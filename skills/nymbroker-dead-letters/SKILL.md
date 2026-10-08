---
name: nymbroker-dead-letters
description: Find, inspect and replay failed (dead-lettered) NymBroker messages for each transport — broker dead-letter endpoint, SQL Failed rows, Azure Service Bus dead-letter queue, RabbitMQ dead-letter exchange — and build a replay consumer or admin tool using IMessageContext.DeadLetter. Use when the user asks where failed messages went, how to see why a message failed, how to retry, resubmit, replay or purge dead letters / poison messages, or wants a dead-letter handling process.
---

# NymBroker dead letters: inspect and replay

A message is dead-lettered when it can never succeed (undecodable, expired, unknown compression) or when its consumer kept failing until the transport's retry limit. Where it ends up depends on the **source endpoint**:

| Source endpoint | Retries | Dead letters go to | Reason stored in |
|---|---|---|---|
| SQLite / PostgreSQL / SQL Server | `MaxRetryCount` (default 5) | same table, `status = 3` (Failed) | `last_error` / `LastError` |
| Azure Service Bus | entity `MaxDeliveryCount` | the queue's / subscription's dead-letter queue | Service Bus reason + description |
| RabbitMQ | one redelivery | the queue's dead-letter exchange — **dropped if none is configured** | `x-death` header (reason `rejected`); NymBroker's reason only in logs |
| Memory / File, or `UseNativeDeadLetter = false` | none | the broker's `WithDeadLetterEndpoint` endpoint — **only logged if none is set** | `deadLetter` block in the envelope |

Every dead-lettering is logged at `Warning` with its reason and counted in `nymbroker.messages.dead_lettered` (tags `reason`, `source`, `mode`).

Docs: https://github.com/nymankla/NymBroker/blob/master/docs/reliability.md

## 1. Clarify

From the registration code: which endpoints receive, their transports, whether `WithDeadLetterEndpoint` / `UseNativeDeadLetter` are set. Ask the user whether they want to **look** (one-off inspection), **replay** (one-off or a tool), or **set up** a durable dead-letter process. Replaying causes side effects again — confirm before running anything against production data.

## 2. Reasons

`DeadLetterReasons` (namespace `NymBroker.Core.Endpoint`): `ConsumerFailed`, `TopicDeliveryFailed`, `DeserializationFailed`, `Expired`, `UnknownCompression`; plus transport reasons such as Service Bus `MaxDeliveryCountExceeded`. Only `ConsumerFailed` / `TopicDeliveryFailed` / `MaxDeliveryCountExceeded` are usually worth replaying after a fix; the others need the data or the sender fixed.

## 3. Per transport

### Broker dead-letter endpoint (Memory, File, opt-out)

Make sure one exists and is `WriteOnly` in the main app (otherwise failures loop):

```csharp
.AddFileEndPoint("DeadLetters", new FileSettings { PostPath = "dead-letters" }, EndpointMode.WriteOnly)
.WithDeadLetterEndpoint("DeadLetters")
```

Each dead letter is the original envelope plus a `deadLetter` block (`reason`, `description`, `exceptionType`, `sourceEndpoint`, `deadLetteredAt`, `deliveryCount`). For a File endpoint, inspect the JSON files directly. Bytes that weren't JSON arrive as type `nymbroker.undecodable` (`UndecodableMessage` with `PayloadBase64` / `PayloadText`).

To replay, read them with a **separate** process or broker that registers the same endpoint as readable (for File: `ReadPath = "dead-letters"`) and a replay consumer (section 4).

### SQL endpoints

```sql
-- PostgreSQL / SQL Server (default table nymbroker_messages / dbo.nymbroker_messages)
SELECT queue_id, attempt_count, last_error, failed_at_utc FROM nymbroker_messages WHERE status = 3;

-- replay after fixing the cause: back to Pending
UPDATE nymbroker_messages
SET status = 0, attempt_count = 0, last_error = NULL, failed_at_utc = NULL
WHERE status = 3 AND last_error LIKE 'ConsumerFailed%';

-- SQLite (default table NymBrokerMessages)
UPDATE NymBrokerMessages SET Status = 0, AttemptCount = 0, LastError = NULL, FailedAtUtc = NULL WHERE Status = 3;
```

`last_error` starts with the reason for dead-lettered rows (`"{Reason}: {Description}"`); for retry exhaustion it holds the last exception. Completed and Failed rows are never deleted by NymBroker — suggest a periodic cleanup (`DELETE … WHERE status IN (2, 3) AND completed_at_utc/failed_at_utc < …`) if the table grows.

### Azure Service Bus

Add a second endpoint on the same queue (or topic + subscription) that reads the dead-letter queue, with a consumer:

```csharp
.AddAzureServiceBusEndPoint("OrdersDlq", new AzureServiceBusSettings
{
    ConnectionString = cs, QueueName = "orders", ReadDeadLetterQueue = true
})
```

Its messages carry `context.DeadLetter` filled from Service Bus's reason and description. Returning normally **removes** the message from the DLQ; throwing abandons it (it stays). Run this endpoint only when replaying, or in a separate admin process, unless an automatic policy is intended. Service Bus Explorer in the Azure portal also shows and resubmits DLQ messages.

### RabbitMQ

Check the queue has `x-dead-letter-exchange` set; dead letters are in the queue bound to that exchange. Read them with a separate `RabbitMqEndPoint` (`ReadQueueName` = the dead-letter queue) and a replay consumer, or shovel them back in the management UI.

## 4. Replay consumer

```csharp
using NymBroker.Core.Consume;
using NymBroker.Core.Endpoint;
using NymBroker.Core.Impl;
using NymBroker.Core.Message;

public sealed class OrderCreatedReplay(INymBroker broker, ILogger<OrderCreatedReplay> logger) : IConsume<OrderCreated>
{
    public async Task ConsumeAsync(OrderCreated message, IMessageContext context, CancellationToken ct = default)
    {
        var dl = context.DeadLetter;
        if (dl?.Reason is DeadLetterReasons.ConsumerFailed or DeadLetterReasons.TopicDeliveryFailed or "MaxDeliveryCountExceeded")
        {
            await broker.PostAsync("Orders", message, ct);   // a fresh envelope; failing again dead-letters it again
            logger.LogInformation("Replayed {MessageId} ({Reason}: {Description})", context.Id, dl.Reason, dl.Description);
        }
        else
        {
            logger.LogWarning("Not replaying {MessageId}: {Reason} {Description}", context.Id, dl?.Reason, dl?.Description);
        }
    }
}
```

- One consumer per message type per broker — that's why replay usually runs in its own process (or a separate broker instance) next to the normal consumers.
- Replay posts a **new** envelope (new id), so duplicate detection won't block it; make sure the consumer is idempotent.
- Add a guard (count, `deadLetteredAt` age, dry-run flag) so a still-broken consumer doesn't ping-pong forever.

## 5. Finish

Tell the user where dead letters for each of their endpoints end up, how to see them, and how to replay; point out any gap (no dead-letter endpoint for Memory/File, no DLX in RabbitMQ, unbounded Completed/Failed rows).
