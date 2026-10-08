---
name: nymbroker-troubleshoot
description: Diagnose NymBroker problems in an application — messages not consumed, "No endpoint registered", unknown message type, consumer never called, messages processed repeatedly or looping, rows stuck InProgress or Failed, growing backlog, startup exceptions, duplicate consumers, health check unhealthy. Use when the user reports that NymBroker, a queue, a consumer or message delivery is not working, slow, stuck, duplicated or throwing.
---

# Troubleshoot NymBroker

Work from evidence: read the registration code, the logs and the transport's state before changing anything. Explain the cause to the user, then fix it.

Docs: https://github.com/nymankla/NymBroker/blob/master/docs/user-guide.md

## 1. Collect facts

1. **Registration**: find the `AddNymBroker()` chain. Note every endpoint (name, transport, `EndpointMode`), consumers, topics, `WithDeadLetterEndpoint`, idempotency, and the code adding routes (`broker.Route…`) and filters (`broker.AddFilter`). Check `.Build()` is called once.
2. **Logs**: ask for or find them (console, Application Insights, Seq…). Useful lines:
   - `Endpoint 'X' registered (ReadWrite)` / `Started listening on endpoint 'X'` (Information) — the endpoint exists and listens.
   - Each processed message has a log scope with `MessageId`, `CorrelationId`, `MessageType`, `SourceEndpoint` — enable scopes (`"IncludeScopes": true` for the console logger) to correlate.
   - For a closer look, add `.AddMessageLoggingFilter()` on the builder and set `NymBroker` to `Debug`: every received message is logged with its payload. Remove it afterwards (it logs payloads).
3. **Transport state**: SQL queue table rows by status (queries in section 3), RabbitMQ management UI, Service Bus Explorer, the File endpoint's folders.
4. **Versions**: all `NymBroker*` packages must have the same version (`dotnet list package`).

## 2. Symptoms

| Symptom / message | Likely cause | Fix |
|---|---|---|
| `No endpoint registered with name 'X'` | Typo, endpoint not registered, or a config entry whose `With…()` is missing (`WithSql`, `WithPostgres`, `WithSqlServer`, `WithRabbitMq`, `WithAzureServiceBus`) — unknown config entries are silently ignored | Register it / add the `With…()` call |
| `Endpoint 'X' is read-only and cannot receive posted messages` | Posting to a `ReadOnly` endpoint | Use `ReadWrite` or `WriteOnly` |
| Startup fails: `Route targets read-only endpoint 'X'` / `Topic '…' targets read-only endpoint` / `Dead letter endpoint 'X' is read-only` / `Wire tap endpoint 'X' is read-only` | A destination is registered `ReadOnly` | Make destinations `WriteOnly` (or `ReadWrite`) |
| Warning `No consumer registered for message type 'T'` | No `AddConsumer<…>()` for that type in this process | Register the consumer (nymbroker-consumer) |
| Warning `No consumer or route for unresolved message type 'name'` | The envelope's `messageType` doesn't match any type known here: `[MessageName]` differs between sender and receiver, missing on one side (full type names differ across assemblies), or the type is only used for sending here | Same `[MessageName("…")]` on both sides (shared contracts); register a consumer/route for it |
| Consumer never called, no warnings | A route or topic matched first (they take precedence over consumers); a filter returned `null`; duplicate detection dropped it (`nymbroker.messages.duplicates`); the endpoint is `WriteOnly`; the host/broker is not started | Check routes/topics/filters; use `ReadWrite`; make sure the host runs |
| `InvalidOperationException` at `AddConsumer` naming two consumers | Two consumers for one message type (one per type allowed), or two consumer classes with the same class name | Merge them, or use a topic with `ISubscribe<T>` subscribers; rename the class |
| `NymBrokerBuilder.Build() can only be called once.` | `Build()` called twice / `AddNymBroker()` registered twice | One chain, one `Build()` |
| Same message processed over and over, CPU high | Routing loop: a route/topic sends to an endpoint this broker also listens on | Destination `WriteOnly`, or guard with `WhenFrom`/`WhenNotFrom` (nymbroker-routing) |
| Consumer runs several times for one message | Consumer threw → transport retried (expected), or at-least-once redelivery after crash/lease expiry | Make the consumer idempotent; enable `AddIdempotentReceiver` or a durable store; find the exception in the logs |
| SQL rows stuck `InProgress` (status 1) | Process died mid-message; they return after `LeaseTimeout` (default 5 min). A handler slower than the lease causes overlapping deliveries | Wait, or raise `LeaseTimeout` above the slowest handler |
| SQL rows `Failed` (status 3) | Consumer threw `MaxRetryCount` times (default 5), or dead-lettered (undecodable, expired) — reason in `last_error` / `LastError` | Fix the cause, then replay (nymbroker-dead-letters) |
| Backlog grows | Consumer too slow or failing; one instance only; `PollInterval`/`BatchSize` too small; Service Bus `MaxConcurrentCalls = 1` | Check `nymbroker.message.processing.duration` and failures; scale out (PostgreSQL/SQL Server/RabbitMQ/Service Bus), raise `BatchSize` / `MaxConcurrentCalls` (gives up ordering) |
| Messages lost after restart (Memory / File) | Memory is in-process; Memory/File never redeliver once taken | Use a durable transport; add `WithDeadLetterEndpoint` for failures |
| Failures disappear (Memory / File) | No dead-letter endpoint → failures are only logged | `WithDeadLetterEndpoint("DeadLetters")` with a `WriteOnly` endpoint |
| RabbitMQ failures vanish | Rejected messages are dropped unless the queue has a dead-letter exchange | Configure a DLX on the queue |
| Split (large) messages never processed | Parts went to different instances (reassembly is in memory, per instance) or are incomplete (discarded after 2 h) | Don't split with competing consumers; keep messages under the transport limit |
| Health check `Unhealthy` | Broker not started, or an endpoint can't reach its server (`report.Endpoints[*].Message`) | Fix connectivity; mark optional endpoints `ConfigureHealthCheck(o => o.NonCritical("X"))` |
| SQLite `database is locked` / slow with several machines | SQLite is for one host | PostgreSQL or SQL Server for multi-instance |

## 3. Inspect SQL queue tables

Status: `0` Pending, `1` InProgress, `2` Completed, `3` Failed. Default tables: SQLite `NymBrokerMessages`, PostgreSQL `nymbroker_messages`, SQL Server `dbo.nymbroker_messages` (or the configured `TableName`).

```sql
-- PostgreSQL / SQL Server
SELECT status, COUNT(*) FROM nymbroker_messages GROUP BY status;
SELECT queue_id, attempt_count, last_error, failed_at_utc FROM nymbroker_messages WHERE status = 3 ORDER BY queue_id DESC;
-- payload as text: PostgreSQL convert_from(payload, 'UTF8'); SQL Server CAST(payload AS VARCHAR(MAX))

-- SQLite
SELECT Status, COUNT(*) FROM NymBrokerMessages GROUP BY Status;
SELECT QueueId, AttemptCount, LastError, CAST(Payload AS TEXT) FROM NymBrokerMessages WHERE Status = 3;
```

The payload is the JSON envelope; compare its `messageType` with the receiver's `[MessageName]`.

## 4. Report

Tell the user: the cause (with the log line or row that proves it), the fix you made or propose, and how to verify (a log line, a status count, a test). If the evidence doesn't explain it, say what is missing and how to get it (more logging, `AddMessageLoggingFilter`, a reproduction with Memory endpoints).
