# NymBroker.ConsumerSample

A long-running worker service that polls a shared SQLite queue for `OrderMessage` records and logs each one. Runs until stopped with Ctrl+C.

Pairs with [NymBroker.ProducerSample](../NymBroker.ProducerSample/README.md), which writes messages into the same database file.

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
NymBroker Consumer Sample
Transport : sqlite
Source    : Data Source=C:\Users\you\nymbrokersample\nymbroker-queue.db
Press Ctrl+C to stop.

info: NymBroker.Core.Impl.NymBrokerImpl[0]
      Endpoint 'Queue' registered (ReadWrite)
info: NymBroker.ConsumerSample.Consumers.OrderConsumer[0]
      Received order ORD-20260101120000-001 | Customer: Alice | Amount: $9.99 | Priority: high
```

## Transports

| `--transport` | Prerequisite | Source |
|---|---|---|
| `sqlite` (default) | none | `~/nymbrokersample/nymbroker-queue.db`, table `Orders` |
| `postgres` | PostgreSQL on `localhost` — start with `./setup-postgres.ps1` | `nymbroker` database, table `orders` |
| `rabbit` | RabbitMQ on `localhost` — start with `./setup-rabbitmq.ps1` | queue `orders` |

## Configuration (SQLite / PostgreSQL)

| Setting | Value |
|---|---|
| Poll interval | 500 ms |
| Batch size | 100 (SQLite) / 10 (PostgreSQL) |
| Retry on failure | 3 attempts before marking a message `Failed` |

## Message lifecycle

Each message written by the producer starts as `Pending`. The consumer claims it (`InProgress`), dispatches it to `OrderConsumer`, then marks it `Completed`. If the handler throws, the message is returned to `Pending` and retried up to `MaxRetryCount` times before being marked `Failed`.
