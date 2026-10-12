# NymBroker.Endpoint.RabbitMq

`RabbitMqEndPoint` — `autoAck: false`; every message is settled by its `ProcessResult`:

- `Completed` → ack, batched by `BatchAckSize`.
- `Retry` → nack `requeue: true`, or `requeue: false` once already redelivered (`RejectRedeliveredFailures`) — poison protection.
- `DeadLetter` → nack `requeue: false`, so it reaches the queue's dead-letter exchange if configured (RabbitMQ records `x-death` reason `rejected`; our reason is only logged).
- Handler exception → `LogError`, treated as `Retry`. Settle failure → `LogError` (redelivered). Exceptions escaping a consumer callback are logged via the channel's `CallbackExceptionAsync` (RabbitMQ.Client would swallow them).

**Connections**: connection and publish-channel setup use `SemaphoreSlim(1,1)` with a double-check to prevent concurrent initialisation. Reconnect uses Core's `RetryPolicy`; each reconnect resets the ack batch (delivery tags belong to the old channel).

**TLS**: `UseTls` / `TlsServerName` / `ClientCertificatePath` + `ClientCertificatePassword` map to `ConnectionFactory.Ssl` in the internal static `CreateConnectionFactory`. `AcceptablePolicyErrors` stays `None` — the server certificate is always verified. **No opt-out; don't add one.** Default settings are the local container's (dev-only, see [docs/security.md](../docs/security.md)).

**Stopping**: the listener runs on its own linked `CancellationTokenSource` (the broker's start token is never cancelled by a stop). `StopListeningAsync`: `BasicCancelAsync` the consumer → cancel the token (handler and reconnect loop see it) → wait for the delivery in progress (`_deliveryLock`, held while a delivery is handled and settled) → ack the pending batch → await the loop → close the consume channel. Deliveries dispatched after the stop began are skipped and left unacked, so RabbitMQ requeues them on close. Settlement uses `CancellationToken.None`, so a handled message is acked even while stopping. Starting an endpoint that is already listening throws. Unexpected loop end → `LogCritical`; failing to cancel/ack/close on stop → `LogWarning`.

Integration tests: `RabbitMqEndPointTests` (`NYMBROKER_RABBITMQ_HOST`, `./scripts/setup-rabbitmq.ps1`). A refused localhost connection takes ~2 s on Windows — reconnect tests measure the interval between attempts rather than assume one.
