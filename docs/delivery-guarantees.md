# Delivery guarantees and endpoint capabilities

[← User guide](user-guide.md)

Choose an endpoint based on where its data lives and how it settles a received message. NymBroker does not provide exactly-once processing: the transport and the consumer's side effects are not committed in one transaction.

## Delivery semantics

Delivery is **at least once only when the source transport can redeliver and is configured to do so**. RabbitMQ, Azure Service Bus, SQLite, PostgreSQL and SQL Server can retry a message after a handler failure or an interrupted delivery. Redelivery can repeat consumer side effects, so consumers should be idempotent. A durable transport does not by itself make consumer side effects exactly once.

Memory and File do not redeliver after handing a message to the broker. Memory removes the item from its in-process channel before calling the handler. File renames the input to `.processed` before calling the handler. A failure after either boundary is logged; it is not automatically retried by that endpoint. Configure a broker dead-letter endpoint to retain failures where applicable, and make file-side effects recoverable if a process can stop during handling.

### Consume and settlement boundaries

| Endpoint type | When the source is settled | What a crash can mean |
|---|---|---|
| Memory | The item is read from the channel before handler invocation. | A process restart loses queued items. A failure or crash after the read does not cause redelivery. |
| File | The input is renamed to `.processed` before handler invocation. | An unprocessed or failed file is not automatically picked up again. Pending input files remain on disk across a process restart, subject to filesystem behavior; `.processed` files require operator recovery or a separate replay process. |
| SQLite, PostgreSQL, SQL Server | A claimed row is marked completed/failed, or returned to pending, after handling. Claims use leases; an interrupted claim can be retried after lease expiry. | A crash after consumer side effects but before settlement can repeat those effects. A crash before handling leaves the claim to expire and be delivered again. |
| RabbitMQ | The message is acknowledged after handling (successful acknowledgements may be batched); failures are negatively acknowledged. | If the broker does not receive an acknowledgement, it can redeliver the message. A crash after consumer side effects but before acknowledgement can repeat them. |
| Azure Service Bus | The peek-locked message is completed, abandoned, or dead-lettered after handling. | If settlement fails or the lock expires before settlement, Service Bus can deliver it again. A crash after consumer side effects but before completion can repeat them. |

Graceful shutdown waits for in-flight processing where the endpoint supports it, but it cannot make a consumer's external side effects atomic with transport settlement. Abrupt termination follows the crash behavior above. For SQL queues, use a lease timeout appropriate to the maximum handler duration; a handler that outlives its lease can overlap a later delivery.

### Ordering

NymBroker does **not** guarantee end-to-end ordering. A retry can be delivered after later messages; competing consumers and parallel processing can complete messages in a different order; filesystem enumeration and database polling are not a global ordering contract. Azure Service Bus with `MaxConcurrentCalls` greater than one explicitly permits concurrent handling. If order matters, partition or serialize work at the application/transport level and make retries safe.

## Endpoint capability matrix

“Durable” describes the endpoint's backing store, not every configuration or failure mode. Database and broker durability also depend on the server's persistence and recovery configuration.

| Endpoint | Data durability | Multi-node suitability | Native dead-letter support | Recommended use and limitations |
|---|---|---|---|---|
| **Memory** | No; held only in process memory. | No. | No; failures can go to the broker dead-letter endpoint. | Tests and transient in-process work. A restart loses queued items, and a consumed item is not redelivered after failure. |
| **File** | Files persist according to the local filesystem and storage configuration. | No shared-directory coordination guarantee; use one consumer instance per input directory. | No; failures can go to the broker dead-letter endpoint. | File drops and simple integrations. Inputs are renamed to `.processed` before handling, so handler failures are not automatically retried; filesystem order is not guaranteed. |
| **SQLite** | Yes for a file database, subject to SQLite settings and storage. Default WAL with `Synchronous.Normal` survives an application crash, but the latest commits can be lost on power loss; use `Full` for stricter durability. `:memory:` is volatile. | **Single host only**; do not use as a multi-node shared queue or place the database on a shared network filesystem. | Yes; terminal failures are kept as `Failed` rows (default `UseNativeDeadLetter = true`). | Local/single-host applications and edge devices. SQLite locking and a local database are not a substitute for a shared multi-node queue. |
| **PostgreSQL** | Yes, subject to PostgreSQL durability configuration. | Yes; multiple instances can claim rows safely. | Yes; terminal failures are retained as `Failed` rows (default enabled). | Shared durable queues for deployments already using PostgreSQL. |
| **SQL Server** | Yes, subject to SQL Server durability configuration. | Yes; multiple instances can claim rows safely. | Yes; terminal failures are retained as `Failed` rows (default enabled). | Shared durable queues for deployments already using SQL Server. |
| **RabbitMQ** | The endpoint declares durable queues, but publishes do not explicitly mark messages persistent; do not assume messages survive a broker restart. | Yes, with a shared broker and suitable queue topology. | Only if the queue has a dead-letter exchange configured; poison messages are rejected without requeue. | Existing RabbitMQ deployments. By default a failed message is requeued once; another failure is rejected. Configure a dead-letter exchange if rejected messages must be retained. |
| **Azure Service Bus** | Yes; backed by the managed Service Bus entity. | Yes; multiple consumers can use the same queue or subscription. | Yes; the entity's dead-letter queue is used (default enabled). | Managed Azure queues and topics. Delivery is lock-based; parallel calls and lock expiry can cause concurrent or repeated handling. |

Native dead-letter handling is enabled by default for RabbitMQ, Azure Service Bus and SQL endpoints. Setting `UseNativeDeadLetter = false` sends eligible failures to `WithDeadLetterEndpoint` instead; the source is then completed rather than retried for those failures. RabbitMQ still needs a configured dead-letter exchange for rejected messages to be retained by RabbitMQ. See [Reliability](reliability.md) for retry limits, broker dead-lettering and duplicate detection.

For duplicate detection across restarts or instances, use a shared PostgreSQL or SQL Server idempotency store. SQLite idempotency storage, like the SQLite endpoint, is for **one host**, not multiple machines. In-memory idempotency is process-local. Idempotency reduces duplicate effects but cannot provide exactly-once processing when consumer side effects are outside the idempotency store's transaction.
