# NymBroker.ProducerSample

A CLI producer that writes `OrderMessage` records to a shared queue and exits immediately after each batch.

Demonstrates `EndpointMode.WriteOnly`: the broker registers the endpoint for posting only — no listener or poll loop is started, so the process exits cleanly as soon as the messages are written.

## Usage

Can be run from any directory.

```bash
# Post 3 messages via SQLite (default)
dotnet run --project samples/NymBroker.ProducerSample

# Post N messages
dotnet run --project samples/NymBroker.ProducerSample -- --count 10

# Choose transport
dotnet run --project samples/NymBroker.ProducerSample -- --count 5 --transport sqlite
dotnet run --project samples/NymBroker.ProducerSample -- --count 5 --transport postgres
dotnet run --project samples/NymBroker.ProducerSample -- --count 5 --transport rabbit
```

The process exits after each batch. Run it multiple times to accumulate messages in the queue.

## Example output

```
info: Program[...]
      ProducerSample starting — transport=rabbit count=5
info: Program[...]
      Posted ORD-0001 Dave 282.67 [high]
info: Program[...]
      Posted ORD-0002 Carol 337.70 [high]
info: Program[...]
      Posted ORD-0003 Dave 1026.33 [high]
info: Program[...]
      Posted ORD-0004 Dave 469.94 [high]
info: Program[...]
      Posted ORD-0005 Carol 641.15 [normal]
info: Program[...]
      5 order(s) posted via rabbit
```

## Transports

| `--transport` | Prerequisite | Queue |
|---|---|---|
| `sqlite` (default) | none | `consumer-sample.db`, table `NymBrokerMessages` |
| `postgres` | PostgreSQL on `localhost` — start with `./setup-postgres.ps1` | `nymbroker` database, table `consumer_sample` |
| `rabbit` | RabbitMQ on `localhost` — start with `./setup-rabbitmq.ps1` | queue `consumer.sample` |

The endpoint is always registered as `WriteOnly` — no listener is started and the process exits as soon as the batch is written.

## Pairing with the consumer

Start [NymBroker.ConsumerSample](../NymBroker.ConsumerSample/README.md) with the same `--transport` flag in a separate terminal. The two processes can run in any order — all three transports persist messages between runs.
