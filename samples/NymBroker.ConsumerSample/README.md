# NymBroker.ConsumerSample

A long-running worker service that listens for `OrderMessage` records and logs each one. Runs until stopped with Ctrl+C.

Pairs with [NymBroker.ProducerSample](../NymBroker.ProducerSample/README.md), which writes messages into the same queue.

## Usage

Can be run from any directory.

```bash
# Listen via SQLite (default)
dotnet run --project samples/NymBroker.ConsumerSample

# Choose transport
dotnet run --project samples/NymBroker.ConsumerSample -- --transport sqlite
dotnet run --project samples/NymBroker.ConsumerSample -- --transport postgres
dotnet run --project samples/NymBroker.ConsumerSample -- --transport rabbit
```

Use the same `--transport` flag as the producer. The consumer runs until stopped with Ctrl+C.

Example output:

```
ConsumerSample started (transport: rabbit). Waiting for messages — Ctrl+C to stop.

info: NymBroker.ConsumerSample.Consumers.OrderConsumer[0]
      Received order ORD-0001 from Alice — £499.00 [high]
info: NymBroker.ConsumerSample.Consumers.OrderConsumer[0]
      Received order ORD-0002 from Bob — £29.99 [normal]
```

## Transports

| `--transport` | Prerequisite | Source |
|---|---|---|
| `sqlite` (default) | none | `consumer-sample.db`, table `NymBrokerMessages` |
| `postgres` | PostgreSQL on `localhost` — start with `./setup-postgres.ps1` | `nymbroker` database, table `consumer_sample` |
| `rabbit` | RabbitMQ on `localhost` — start with `./setup-rabbitmq.ps1` | queue `consumer.sample` |

## Message lifecycle (SQLite / PostgreSQL)

Each message written by the producer starts as `Pending`. The consumer claims it (`InProgress`), dispatches it to `OrderConsumer`, then marks it `Completed`. If the handler throws, the message is returned to `Pending` and retried up to `MaxRetryCount` times before being marked `Failed`.

For RabbitMQ, messages are manually ack'd on success and nack'd with requeue on failure — the broker handles redelivery.
