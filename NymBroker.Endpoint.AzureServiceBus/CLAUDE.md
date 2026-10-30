# NymBroker.Endpoint.AzureServiceBus

`AzureServiceBusEndPoint` — push-based. A `ServiceBusProcessor` (peek-lock, `AutoCompleteMessages = false`, `MaxConcurrentCalls` default 1) calls the broker and settles by the `ProcessResult` in `ServiceBusSettlement` (internal, unit-tested through `IServiceBusMessageSettler`). `Azure.Messaging.ServiceBus` only — no `Azure.Identity` dependency.

- `Completed` → `CompleteMessageAsync`; `Retry` (or handler exception) → `AbandonMessageAsync` — Service Bus redelivers and dead-letters with `MaxDeliveryCountExceeded` at the entity's `MaxDeliveryCount`; `DeadLetter` → `DeadLetterMessageAsync(reason, description)` (truncated to 4 KB).
- `ReadDeadLetterQueue = true` receives from the DLQ sub-queue and annotates the envelope (`DeadLetterEnvelope.Annotate`); there a `DeadLetter` result **completes** the message (already dead-lettered) with a warning.
- One lazily created `ServiceBusClient` + `ServiceBusSender` per endpoint (thread-safe, long-lived). Transient faults use the SDK's retry options — no `RetryPolicy` wrapper. `ProcessErrorAsync` logs every processor error. `PostBatchAsync` uses `ServiceBusMessageBatch` chunks (atomic per chunk).
- Auth: `ConnectionString`, or `FullyQualifiedNamespace` + `Credential` (`TokenCredential`, code-only, `[JsonIgnore]`). `AzureServiceBusSettings.Validate` runs at registration and in the constructor.
- `HealthCheckAsync()` peeks the entity (or opens the sender for a send-only topic) with a 5 s timeout; unhealthy if the processor stopped unexpectedly.
- `StopListeningAsync` → `StopProcessingAsync` (waits for in-flight handlers). A message whose handler finished is **always settled with `CancellationToken.None`** — stopping cancels the processor's token and would otherwise leave it to be redelivered as a duplicate. A handler cancelled during shutdown leaves its message unsettled (redelivered after the lock expires). If `StartProcessingAsync` fails the processor is disposed.

Emulator quirks: `ReceiveAndDelete` from a DLQ returned nothing (peek-lock works); `DeadLetterSource` is empty. Integration tests (`NYMBROKER_SERVICEBUS_CS`, `./scripts/setup-servicebus.ps1`) share the fixed queue `nymbroker.tests` (MaxDeliveryCount 3) and drain it before each test until a peek on a fresh receiver finds nothing — a single empty receive can be a slow link or a still-locked message.
