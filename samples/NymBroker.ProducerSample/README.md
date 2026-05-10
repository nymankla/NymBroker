# NymBroker.ProducerSample

A CLI producer that writes `OrderMessage` records to a shared SQLite queue and exits immediately after each batch.

Demonstrates `EndpointMode.WriteOnly`: the broker registers the endpoint for posting only — no listener or poll loop is started, so the process exits cleanly as soon as the messages are written.

## Usage

Can be run from any directory.

```bash
# Post 1 message via SQLite (default)
dotnet run --project samples/NymBroker.ProducerSample

# Post N messages
dotnet run --project samples/NymBroker.ProducerSample -- 5

# Choose transport
dotnet run --project samples/NymBroker.ProducerSample -- 5 --transport sqlite
dotnet run --project samples/NymBroker.ProducerSample -- 5 --transport postgres
dotnet run --project samples/NymBroker.ProducerSample -- 5 --transport rabbit
```

The process exits after each batch. Run it multiple times to accumulate messages in the queue.

## Transports

| `--transport` | Prerequisite | Queue |
|---|---|---|
| `sqlite` (default) | none | `~/nymbrokersample/nymbroker-queue.db`, table `Orders` |
| `postgres` | PostgreSQL on `localhost` — start with `./setup-postgres.ps1` | `nymbroker` database, table `orders` |
| `rabbit` | RabbitMQ on `localhost` — start with `./setup-rabbitmq.ps1` | queue `orders` |

The endpoint is always registered as `WriteOnly` — no listener is started and the process exits as soon as the batch is written.

## Pairing with the consumer

Start [NymBroker.ConsumerSample](../NymBroker.ConsumerSample/README.md) with the same `--transport` flag in a separate terminal. The two processes can run in any order — all three transports persist messages between runs.
